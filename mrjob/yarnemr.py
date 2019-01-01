# -*- coding: utf-8 -*-
# Copyright 2009-2019 Yelp and Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import division
import logging
import operator
import os
import random
import re
import time
from datetime import datetime
from datetime import timedelta
from urllib3.exceptions import HTTPError

from mrjob.aws import _boto3_paginate
from mrjob.compat import version_gte
from mrjob.emr import _attempt_to_acquire_lock
from mrjob.emr import _EMR_STATES_STARTING
from mrjob.emr import _EMR_STATUS_RUNNING
from mrjob.emr import _EMR_STATUS_TERMINATING
from mrjob.emr import _make_lock_uri
from mrjob.emr import _POOLING_SLEEP_INTERVAL
from mrjob.emr import EMRJobRunner
from mrjob.step import _is_spark_step_type
from mrjob.step import StepFailedException
from mrjob.yarn_api import YarnResourceManager


log = logging.getLogger(__name__)


# Amount of time in seconds before we timeout yarn api calls.
_YARN_API_TIMEOUT = 20

# We lock scheduling on an entire cluster while scheduling a job on it. This
# is unlike the EMR cluster which locks at a per-step granularity. As such,
# there is a risk of a deadlock forming if a running process holding a lock is
# killed. Therefore, rather than releasing the lock we simply timeout the lock
# after 30 seconds. Since the lock is only needed to reduce scheduling
# contention this method is sufficient.
_LOCK_TIMEOUT = 30

# The amount of time to wait before re-querying clusters while there are still
# valid clusters remaining (unlike :py:data:mrjob.emr._POOLING_SLEEP_INTERVAL
# which is the time to wait when there are no valid clusters remaining).
_POOLING_JITTER = 3

# Do not schedule on a cluster if there are more than this many yarn apps
# pending or running, respectively.
_MAX_APPS_PENDING = 2
_MAX_APPS_RUNNING = 50

# Approximate maximum time in seconds to wait while a new cluster is starting
# and bootstrapping. This is approximate since we only wait to the closest
# multiple of the option `check_cluster_every` rounded down.
_NEW_CLUSTER_WAIT_TIME = 1200  # 20 minutes


class _ResourceConstraint(object):
    """Abstracts a constraint on a resource. Checks whether a metric value
    hold under an operator and threshold."""

    def __init__(self, operation, metrics_key, threshold):
        self.operation = operation
        self.metrics_key = metrics_key
        self.threshold = threshold

    def _failure_callback(self, current_metric):
        """Called when a metric constraint is not satisfied"""
        log.debug('    metric "{}" did not satisfy {}({},{})'
                  .format(self.metrics_key, self.operation.__name__,
                          current_metric, self.threshold))

    def __call__(self, metric_dict):
        result = self.operation(metric_dict[self.metrics_key], self.threshold)
        if not result:
            self._failure_callback(metric_dict[self.metrics_key])
        return result


class YarnEMRJobRunner(EMRJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` on an EMR cluster by  running
    directly on YARN. Invoked when you run your job with ``-r yarnemr``.
    """
    alias = 'yarnemr'

    OPT_NAMES = EMRJobRunner.OPT_NAMES | {
        'expected_cores',
        'expected_memory'
    }

    def _required_arg(self, name):
        if self._opts[name] is None:
            raise ValueError('The argument "{}" is required for the YARN'
                             ' EMR runner'.format(name))

    def __init__(self, **kwargs):
        """:py:class:`~mrjob.yarnemr.YarnEMRJobRunner` takes the same arguments
        as :py:class:`~mrjob.emr.EMRJobRunner` with just one additional param,
        `expected_memory`."""
        super(YarnEMRJobRunner, self).__init__(**kwargs)

        self._required_arg('expected_cores')
        self._required_arg('expected_memory')
        self._required_arg('ec2_key_pair_file')

        self._ensure_fair_scheduler()

    def _ensure_fair_scheduler(self):
        """Ensures the yarn-site section of the emr_configurations option
        contains the flag to use the YARN fair scheduler."""
        scheduler_key = 'yarn.resourcemanager.scheduler.class'
        fair_scheduler = 'org.apache.hadoop.yarn.server.resourcemanager.' \
                         'scheduler.fair.FairScheduler'
        found_yarn_config = False
        if self._opts['emr_configurations'] is None:
            self._opts['emr_configurations'] = []
        for conf in self._opts['emr_configurations']:
            if conf['Classification'] == 'yarn-site':
                found_yarn_config = True
                log.debug('Found yarn config, setting yarn scheduler to'
                          ' use fair scheduler.')
                if scheduler_key in conf['Properties']:
                    if conf['Properties'][scheduler_key] != fair_scheduler:
                        log.error('Not using yarn fair scheduler.')
                    else:
                        log.debug('Found yarn config, already using fair'
                                  ' scheduler')
                    break
                conf['Properties'][scheduler_key] = fair_scheduler
        if not found_yarn_config:
            log.debug('Did not find yarn config, creating yarn scheduler with'
                      ' entry to use fair scheduler.')
            self._opts['emr_configurations'].append({
                'Classification': 'yarn-site',
                'Properties': {scheduler_key: fair_scheduler}
            })

    def _launch(self):
        """Set up files and then launch our job on the EMR cluster."""
        self._prepare_for_launch()
        self._launch_yarn_emr_job()

    def _launch_yarn_emr_job(self):
        """Create an empty cluster on EMR if needed, sets self._cluster_id to
        the cluster's ID, and submits the job to the cluster via ssh.

        This logic/code is intended to mirror
        :py:meth:`~mrjob.emr.EMRJobRunner._launch_emr_job`; however this is
        intentionally WET as the logic has inevitably started to drift. The
        function was renamed to `_launch_yarnemr_job` to avoid confusion.
        """
        self._create_s3_tmp_bucket_if_needed()

        # try to find a cluster from the pool
        if (self._opts['pool_clusters'] and not self._cluster_id):
            resource_constraints = [
                _ResourceConstraint(operator.ge, 'availableVirtualCores',
                                    self._opts['expected_cores']),
                _ResourceConstraint(operator.ge, 'availableMB',
                                    self._opts['expected_memory']),
                _ResourceConstraint(operator.lt, 'appsPending',
                                    _MAX_APPS_PENDING),
                _ResourceConstraint(operator.lt, 'appsRunning',
                                    _MAX_APPS_RUNNING)
            ]
            cluster_id = self._find_cluster(resource_constraints)
            if cluster_id:
                self._cluster_id = cluster_id

        # create a cluster if we're not already using an existing one
        if not self._cluster_id:
            self._cluster_id = self._create_cluster(persistent=False)
            self._created_cluster = True
            self._wait_for_cluster()
        else:
            log.info('Adding our job to existing cluster %s (%s)' %
                     (self._cluster_id, self._address_of_master()))

        # now that we know which cluster it is, check for Spark support
        if self._has_spark_steps():
            self._check_cluster_spark_support()

        if version_gte(self.get_image_version(), '4.3.0'):
            self._ssh_fs.use_sudo_over_ssh()

        self._execute_job()

    def _lock_uri(self, cluster_id):
        """Lock unique per cluster"""
        return _make_lock_uri(self._opts['cloud_tmp_dir'], cluster_id, 'yarn')

    def _check_cluster_state(self, cluster, resource_constraints):
        """Queries the cluster for basic metrics and checks these against
        a list of constraints.

        :return: The number of available mb on the cluster if the cluster
                 satisfies the constraints and -1 otherwise.
        """
        # TODO: This assumes the node mrjob is running on can directly
        #       access the EMR master node on port 8888. We really
        #       should just run the raw curl over the ssh config, but
        #       since no one else is using this and our security group
        #       allows for this, we will just be lazy.
        host = cluster['MasterPublicDnsName']
        yrm = YarnResourceManager(host, _YARN_API_TIMEOUT)
        try:
            metrics = yrm.get_cluster_metrics()
        except HTTPError:
            log.info('    received exception while querying cluster metrics')
            return -1

        log.debug('Cluster metrics: {}'.format(metrics))
        if all(constraint(metrics) for constraint in resource_constraints):
            log.debug('    OK - valid cluster state')
            return metrics['availableMB']
        else:
            log.debug('    failed resource constraints')
            return -1

    def _usable_clusters(self, valid_clusters, invalid_clusters,
                         locked_clusters, resource_constraints):
        """Get clusters that this runner can join.

        Note: this will update pass-by-reference arguments `valid_clusters`
        and `invalid_clusters`.

        :param valid_clusters: A map of cluster id to cluster info with a valid
                               setup; thus we do not need to check their setup
                               again.
        :param invalid_clusters: A set of clusters with an invalid setup; thus
                                 we skip these clusters.
        :param locked_clusters: A set of clusters managed by the callee that
                                are in a "locked" state.
        :param resource_constraints: A list of various constraints on the
                                     resources of the target cluster.

        :return: list of cluster ids sorted by available memory in
                 descending order
        """
        emr_client = self.make_emr_client()
        req_pool_hash = self._pool_hash()

        # list of (available_mb, cluster_id)
        cluster_list = []

        # Randomly sleep for up to a minute before proceeding. Otherwise
        # we will end up with hundreds of clusters making an EMR describe
        # API call at the same time.
        initial_jitter = random.randint(0, 60)
        log.info('  Initial scheduling jitter, sleeping for {} seconds.'
                 .format(initial_jitter))
        time.sleep(initial_jitter)

        for cluster_summary in _boto3_paginate(
                'Clusters', emr_client, 'list_clusters',
                ClusterStates=_EMR_STATUS_RUNNING):
            cluster_id = cluster_summary['Id']

            # this may be a retry due to locked clusters
            if cluster_id in invalid_clusters or cluster_id in locked_clusters:
                log.debug('    excluded')
                continue

            # if we haven't seen this cluster before then check the setup
            cluster = valid_clusters.get(cluster_id, None)
            if cluster is None:
                cluster = emr_client.describe_cluster(
                    ClusterId=cluster_id)['Cluster']
                if not self._compare_cluster_setup(
                        emr_client, cluster, req_pool_hash):
                    invalid_clusters.add(cluster_id)
                    continue
                valid_clusters[cluster_id] = cluster

            # always check the state
            available_md = self._check_cluster_state(cluster,
                                                     resource_constraints)
            if available_md == -1:
                # don't add to invalid cluster list since the cluster may
                # be valid when we next check
                continue

            # yay this cluster is good!
            cluster_list.append((available_md, cluster_id,))

        return [_id for _, _id in sorted(cluster_list, reverse=True)]

    def _find_cluster(self, resource_constraints):
        """Find a cluster that can host this runner. Prefer clusters with
        more memory.

        Return ``None`` if no suitable clusters exist after waiting
        ``pool_wait_minutes``.
        """
        valid_clusters = {}
        invalid_clusters = set()
        locked_clusters = []

        max_wait_time = self._opts['pool_wait_minutes']
        now = datetime.now()
        end_time = now + timedelta(minutes=max_wait_time)

        log.info('Attempting to find an available cluster...')
        while now <= end_time:
            # remove any cluster from the locked list if it has been there
            # for more than `_LOCK_TIMEOUT` seconds
            locked_clusters = [(c, t) for (c, t) in locked_clusters
                               if now - t < timedelta(seconds=_LOCK_TIMEOUT)]
            cluster_info_list = self._usable_clusters(
                valid_clusters, invalid_clusters,
                {l[0] for l in locked_clusters}, resource_constraints)
            log.debug('  Found {} usable clusters{}{}'.format(
                        len(cluster_info_list),
                        ': ' if cluster_info_list else '',
                        ', '.join(c for c in cluster_info_list)))

            if cluster_info_list:
                for cluster_id in cluster_info_list:
                    status = _attempt_to_acquire_lock(
                        self.fs, self._lock_uri(cluster_id),
                        self._opts['cloud_fs_sync_secs'], self._job_key,
                        _LOCK_TIMEOUT)
                    if status:
                        log.info('Acquired lock on cluster {}'
                                 .format(cluster_id))
                        return cluster_id
                    else:
                        log.info("Can't acquire lock on cluster {}"
                                 .format(cluster_id))
                        locked_clusters.append((cluster_id, now,))
                # sleep for a couple seconds before trying again
                time.sleep(_POOLING_JITTER)
                now += timedelta(seconds=_POOLING_JITTER)
            elif max_wait_time == 0:
                return None
            else:
                log.info('No clusters available in pool {}. Checking again in'
                         ' {} seconds...'.format(self._opts['pool_name'],
                                                 _POOLING_SLEEP_INTERVAL))
                time.sleep(_POOLING_SLEEP_INTERVAL)
                now += timedelta(seconds=_POOLING_SLEEP_INTERVAL)

        return None

    def _wait_for_cluster(self):
        """Since we are not submitting EMR steps (which will wait to run
        by default) we must manually wait till the cluster has finishing
        bootstrapping before proceeding.

        We sadly write our own waiter since 1) botocore's waiters do not work
        with our retry logic and 2) they do not support intermediate logging.
        We closely mirror the logic in :py:meth:`botocore.waiter.Waiter.wait`.
        """
        emr_client = self.make_emr_client()
        cluster_id = self._cluster_id

        sleep_amount = self._opts['check_cluster_every']
        max_attempts = _NEW_CLUSTER_WAIT_TIME // sleep_amount
        num_attempts = 0

        while True:
            num_attempts += 1
            log.info('Checking cluster {} for running state, attempt {} of'
                     ' {}'.format(cluster_id, num_attempts, max_attempts))
            response = emr_client.describe_cluster(
                ClusterId=cluster_id
            )
            state = response['Cluster']['Status']['State']
            if state in _EMR_STATUS_RUNNING:
                master_ip = response['Cluster']['MasterPublicDnsName']
                log.info('Waiting complete, cluster {} reached running'
                         ' state {} ({})'.format(cluster_id, state, master_ip))
                return
            elif state in _EMR_STATUS_TERMINATING:
                raise Exception('Waiting failed, cluster {} reached error'
                                ' state {}'.format(cluster_id, state))
            else:
                assert state in _EMR_STATES_STARTING, 'Reached invalid state'
            if num_attempts >= max_attempts:
                raise Exception('Max attempts exceeded, cluster {} in'
                                ' state {}'.format(cluster_id, state))
            time.sleep(sleep_amount)

    def _run_ssh_on_master(self, cmd_args, desc, stdin=None):
        """"Wraps :py:meth:`~mrjob.fs.ssh.SSHFilesystem._ssh_run` with our
        exception logic.

        The ``desc`` param to be used in info and error logging.
        """

        host = self._address_of_master()
        try:
            log.info('Running {} command over ssh'.format(desc))
            stdout, stderr = self.fs._ssh_run(host, cmd_args, stdin=stdin)
            return stdout, stderr
        except IOError as ex:
            log.info('  A failure occured; printing stderr logs to stdout')
            log.info(str(ex))
            raise StepFailedException(step_desc=desc.capitalize(),
                                      reason='see above logs')

    def _write_to_log_file(self, base, filename, text):
        """Simply write string to the output dir."""
        file_path = os.path.join(base, '{}_{}'.format(self._job_key, filename))
        with open(file_path, 'w') as fp:
            fp.write(text)
        return file_path

    def _special_yarn_args_formatting(self, command_args):
        """We need to do a couple things to the command arguments we get back
        from the bin runner.

        1) Add in spark config to cause command line call to return immediately

        2) Find any cmdenvs references in the command and wrap the arg in
        single quotes to prevent bash from interpolated the variable when
        we run via ssh.
        """
        app_wait_conf = 'spark.yarn.submit.waitAppCompletion'
        if any(arg.startswith(app_wait_conf) for arg in command_args):
            raise ValueError('User should not be setting spark'
                             ' option {}'.format(app_wait_conf))
        if '--conf' in command_args:
            first_conf_index = command_args.index('--conf')
        else:
            first_conf_index = command_args.index('--spark')
        command_args.insert(first_conf_index, '--conf')
        command_args.insert(first_conf_index+1,
                            '{}=false'.format(app_wait_conf))

        cmdenv_var = ['${}'.format(key) for key in self._opts['cmdenv'].keys()]
        for idx, arg in enumerate(command_args):
            if any(var in arg for var in cmdenv_var):
                command_args[idx] = "'{}'".format(arg)

        return command_args

    def _build_step(self, step_num):
        """Borrow logic from :py:meth:`~mrjob.emr.EMRJobRunner._launch_emr_job`
        with modifications to handle spark-only support.
        """
        step = self._get_step(step_num)

        if _is_spark_step_type(step['type']):
            method = self._spark_step_hadoop_jar_step
        elif step['type'] == 'streaming':
            raise AssertionError('Steaming not yet support in yarn runner')
        elif step['type'] == 'jar':
            raise AssertionError('Jar steps not yet support in yarn runner')
        else:
            raise AssertionError('Bad step type: %r' % (step['type'],))

        command_args = method(step_num)['Args']
        assert command_args[0] == 'spark-submit', 'Programmatic error'
        return self._special_yarn_args_formatting(command_args)

    def _execute_job(self):
        """Runs master setup if needed and submits spark job."""
        # spark submit
        if self._num_steps() > 1:
            raise AssertionError('Multiple steps not yet supported in'
                                 ' yarn runner')
        spark_submit_command = self._build_step(0)
        _, stderr = self._run_ssh_on_master(spark_submit_command,
                                            'spark submit')

        # The application ID appears in several places in these logs, but we
        # choose to take it from the log-line where we submit the application
        # to the YARN resource manager
        self._appid = re.search('Submitting application (application[0-9_]*)'
                                ' to ResourceManager', stderr).groups()[0]
        log.info('Application successfully submitted with application id'
                 ' {}'.format(self._appid))

    def _get_application_info(self):
        """Queries the cluster for state of application."""
        # TODO: same to-do as _check_cluster_state (run over ssh
        #       rather than over http)
        host = self._address_of_master()
        yrm = YarnResourceManager(host, _YARN_API_TIMEOUT)
        return yrm.get_application_info(self._appid)

    def _get_yarn_logs(self):
        """Retrieve YARN application logs. We use the `yarn` cli tool since
        it handles log aggregation well."""
        log.info('Attempting to aquire YARN application logs')
        log_request = ['sudo', 'yarn', 'logs', '-applicationId', self._appid]
        try:
            stdout, _ = self._run_ssh_on_master(log_request,
                                                'YARN log retrieval')
            base = ''  # self._opts['yarn_logs_output_base'] - doesn't exist
            file_path = self._write_to_log_file(base, 'yapp.log', stdout)
            log.info('  Wrote YARN application logs to {}'.format(file_path))
        except StepFailedException:
            log.info('  Unable to retrieve YARN application logs :(')

    def _check_for_job_completion(self):
        log.info('Waiting for {} to complete...'.format(self._appid))

        while True:
            time.sleep(self._opts['check_cluster_every'])

            app_info = self._get_application_info()

            # If the job is in not in a finished state then log the progress
            # and continue
            if app_info['state'] not in ('FINISHED', 'FAILED', 'KILLED'):
                # rounds the elapsed time to seconds for better formatting
                elapsed_ms = timedelta(milliseconds=app_info['elapsedTime'])
                elapsed_time = timedelta(seconds=elapsed_ms.seconds)
                progress = app_info['progress']
                log.info('  RUNNING for {0} with {1:.1f}% complete'
                         .format(elapsed_time, progress))
                continue

            # One of the following: UNDEFINED, SUCCEEDED, FAILED, KILLED
            final_status = app_info['finalStatus']
            if final_status == 'SUCCEEDED':
                log.info('  {} reached successful state!'.format(self._appid))
                return
            else:
                log.info('  {} reached failure state {} :('
                         .format(self._appid, final_status))
                # Disable getting yarn logs
                # self._get_yarn_logs()
                tracking_url = app_info['trackingUrl']
                raise StepFailedException(
                        step_desc='Application {}'.format(self._appid),
                        reason='See logs at {}'.format(tracking_url))
