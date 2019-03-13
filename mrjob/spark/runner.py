# Copyright 2019 Yelp and Google, Inc.
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
"""A runner that can run jobs on Spark, with or without Hadoop."""
import logging
import os.path
import posixpath
import re
from shutil import copy2
from subprocess import CalledProcessError
from tempfile import gettempdir

from mrjob.bin import MRJobBinRunner
from mrjob.compat import jobconf_from_dict
from mrjob.conf import combine_dicts
from mrjob.conf import combine_local_envs
from mrjob.fs.composite import CompositeFilesystem
from mrjob.fs.gcs import GCSFilesystem
from mrjob.fs.gcs import google as google_libs_installed
from mrjob.fs.gcs import _is_permanent_google_error
from mrjob.fs.hadoop import HadoopFilesystem
from mrjob.fs.local import LocalFilesystem
from mrjob.fs.s3 import S3Filesystem
from mrjob.fs.s3 import boto3 as boto3_installed
from mrjob.fs.s3 import _is_permanent_boto3_error
from mrjob.hadoop import fully_qualify_hdfs_path
from mrjob.logs.step import _log_log4j_record
from mrjob.parse import is_uri
from mrjob.setup import UploadDirManager
from mrjob.spark import mrjob_spark_harness
from mrjob.step import StepFailedException
from mrjob.util import cmd_line
from mrjob.util import _create_zip_file

log = logging.getLogger(__name__)


class SparkMRJobRunner(MRJobBinRunner):
    """Runs a :py:class:`~mrjob.job.MRJob` on your Spark cluster (with or
    without Hadoop). Invoked when you run your job with ``-r spark``.
    """
    alias = 'spark'

    # other than ``spark_*``, these options are only used for filesystems
    OPT_NAMES = MRJobBinRunner.OPT_NAMES | {
        'aws_access_key_id',
        'aws_secret_access_key',
        'aws_session_token',
        'cloud_fs_sync_secs',
        'cloud_part_size_mb',
        'google_project_id',  # used by GCS filesystem
        'hadoop_bin',
        's3_endpoint',
        's3_region',  # only used along with s3_endpoint
        'spark_deploy_mode',
        'spark_master',
        'spark_tmp_dir',  # where to put temp files in Spark
    }

    # everything except Hadoop JARs
    # streaming jobs will be run using mrjob_spark_harness.py (see #1972)
    _STEP_TYPES = {
        'spark', 'spark_jar', 'spark_script', 'streaming',
    }

    def __init__(self, mrjob_cls=None, **kwargs):
        """Create a spark runner

        :param mrjob_cls: class of the job you want to run. Used for
                          running streaming steps in Spark
        """
        # need to set this before checking steps in superclass __init__()
        self._mrjob_cls = mrjob_cls

        super(SparkMRJobRunner, self).__init__(**kwargs)

        self._spark_tmp_dir = self._pick_spark_tmp_dir()

        # where local files are uploaded into Spark
        if is_uri(self._spark_tmp_dir):
            spark_files_dir = posixpath.join(self._spark_tmp_dir, 'files', '')
            self._upload_mgr = UploadDirManager(spark_files_dir)

        # where to put job output (if not set explicitly)
        if not self._output_dir:
            self._output_dir = posixpath.join(self._spark_tmp_dir, 'output')

        # keep track of where the spark-submit binary is
        self._spark_submit_bin = self._opts['spark_submit_bin']

        # where to store a .zip file containing the MRJob, with a unique
        # module name
        self._job_script_zip_path = None

        # could raise an exception if *hadoop_input_format* and
        # *hadoop_output_format* are set, but support for these these will be
        # added shortly (see #1944)

    def _check_step(self, step, step_num):
        """Don't try to run steps that include commands or use manifests."""
        super(SparkMRJobRunner, self)._check_step(step, step_num)

        if step.get('input_manifest'):
            raise NotImplementedError(
                'spark runner does not support input manifests')

        # we don't currently support commands, but we *could* (see #1956).
        if step['type'] == 'streaming':
            if not self._mrjob_cls:
                raise ValueError(
                    'You must set mrjob_cls to run streaming steps')

            for mrc in ('mapper', 'combiner', 'reducer'):
                if step.get(mrc):
                    if 'command' in step[mrc] or 'pre_filter' in step[mrc]:
                        raise NotImplementedError(
                            "step %d's %s runs a command, but spark"
                            " runner does not support commands" % (
                                step_num, mrc))

    def _run(self):
        self.get_spark_submit_bin()  # find spark-submit up front
        self._create_setup_wrapper_scripts()
        self._upload_local_files()
        self._run_steps_on_spark()

    def _pick_spark_tmp_dir(self):
        if self._opts['spark_tmp_dir']:
            return self.fs.join(self._opts['spark_tmp_dir'], self._job_key)
        else:
            master = self._spark_master() or 'local'
            if master.startswith('local'):  # including local-cluster
                # need a local temp dir
                # add "-spark" so we don't collide with default local temp dir
                return os.path.join(
                    gettempdir(), self._job_key + '-spark')
            else:
                # use HDFS (same default as HadoopJobRunner)
                return posixpath.join(
                    fully_qualify_hdfs_path('tmp/mrjob'), self._job_key)

    def _default_step_output_dir(self):
        return posixpath.join(self._spark_tmp_dir, 'step-output')

    @property
    def fs(self):
        # Spark supports basically every filesystem there is

        if not self._fs:
            self._fs = CompositeFilesystem()

            if boto3_installed:
                self._fs.add_fs('s3', S3Filesystem(
                    aws_access_key_id=self._opts['aws_access_key_id'],
                    aws_secret_access_key=self._opts['aws_secret_access_key'],
                    aws_session_token=self._opts['aws_session_token'],
                    s3_endpoint=self._opts['s3_endpoint'],
                    s3_region=self._opts['s3_region'],
                ), disable_if=_is_permanent_boto3_error)

            if google_libs_installed:
                self._fs.add_fs('gcs', GCSFilesystem(
                    project_id=self._opts['google_project_id']
                ), disable_if=_is_permanent_google_error)

            self._fs.add_fs('hadoop', HadoopFilesystem(
                self._opts['hadoop_bin']))

            self._fs.add_fs('local', LocalFilesystem())

        return self._fs

    # making mr_job_script visible in Spark

    def _job_script_module_name(self):
        """A unique module name to use with the MRJob script."""
        return re.sub(r'[^\w\d]', '_', self._job_key)

    def _create_job_script_zip(self):
        if not self._job_script_zip_path:
            zip_path = os.path.join(self._get_local_tmp_dir(), 'script.zip')
            name_in_zip = self._job_script_module_name() + '.py'

            log.debug('archiving %s -> %s as %s' % (
                self._script_path, zip_path, name_in_zip))
            with _create_zip_file(zip_path) as zip_file:
                zip_file.write(self._script_path, arcname=name_in_zip)

            self._job_script_zip_path = zip_path

        return self._job_script_zip_path

    def _py_files(self):
        """Patch in :py:attr:`_job_script_zip_path`, if running streaming
        steps."""
        py_files = super(SparkMRJobRunner, self)._py_files()

        if self._has_streaming_steps():
            py_files.append(self._create_job_script_zip())

        return py_files

    # running the job

    def _run_steps_on_spark(self):
        steps = self._get_steps()

        for group in self._group_steps(steps):
            step_num = group['step_num']
            last_step_num = step_num + len(group['steps']) - 1

            # the Spark harness can run several streaming steps in one job
            if step_num == last_step_num:
                step_desc = 'step %d' % (step_num + 1)
            else:
                step_desc = 'steps %d-%d' % (step_num + 1, last_step_num + 1)

            log.info('Running %s of %d' % (step_desc, len(steps)))

            self._run_step_on_spark(group['steps'][0], step_num, last_step_num)

    def _group_steps(self, steps):
        """Group streaming steps together."""
        # a list of dicts with:
        #
        # type: shared type of steps
        # steps: list of steps in group
        # step_num: (0-indexed) number of first step
        groups = []

        for step_num, step in enumerate(steps):
            # should we add *step* to existing group of streaming steps?
            if (step['type'] == 'streaming' and groups and
                    groups[-1]['type'] == 'streaming' and
                    step.get('jobconf') ==
                    groups[-1]['steps'][0].get('jobconf')):
                groups[-1]['steps'].append(step)
            else:
                # start a new step group
                groups.append(dict(
                    type=step['type'],
                    steps=[step],
                    step_num=step_num))

        return groups

    def _run_step_on_spark(self, step, step_num, last_step_num=None):
        if self._opts['upload_archives'] and self._spark_master() != 'yarn':
            log.warning('Spark master %r will probably ignore archives' %
                        self._spark_master())

        spark_submit_args = self._args_for_spark_step(step_num, last_step_num)

        env = dict(os.environ)
        env.update(self._spark_cmdenv(step_num))

        returncode = self._run_spark_submit(spark_submit_args, env,
                                            record_callback=_log_log4j_record)

        if returncode:
            reason = str(CalledProcessError(returncode, spark_submit_args))
            raise StepFailedException(
                reason=reason, step_num=step_num, last_step_num=last_step_num,
                num_steps=self._num_steps())

    def _spark_script_path(self, step_num):
        """For streaming steps, return the path of the harness script
        (and handle other spark step types the usual way)."""
        step = self._get_step(step_num)

        if step['type'] == 'streaming':
            return self._spark_harness_path()
        else:
            return super(SparkMRJobRunner, self)._spark_script_path(step_num)

    def _spark_script_args(self, step_num, last_step_num=None):
        """Generate spark harness args for streaming steps (and handle
        other spark step types the usual way).
        """
        step = self._get_step(step_num)

        if step['type'] != 'streaming':
            return super(SparkMRJobRunner, self)._spark_script_args(
                step_num, last_step_num)

        if last_step_num is None:
            last_step_num = step_num

        args = []

        # class name
        args.append('%s.%s' % (self._job_script_module_name(),
                               self._mrjob_cls.__name__))

        # INPUT
        args.append(
            ','.join(self._step_input_uris(step_num)))

        # OUTPUT
        # note that we use the output dir for the *last* step
        args.append(
            self._step_output_uri(last_step_num))

        # --first-step-num, --last-step-num (step range)
        args.extend(['--first-step-num', str(step_num),
                     '--last-step-num', str(last_step_num)])

        # --job-args (passthrough args)
        job_args = self._mr_job_extra_args()
        if job_args:
            args.extend(['--job-args', cmd_line(job_args)])

        # --compression-codec
        jobconf = self._jobconf_for_step(step_num)

        compress_conf = jobconf_from_dict(
            jobconf, 'mapreduce.output.fileoutputformat.compress')
        codec_conf = jobconf_from_dict(
            jobconf, 'mapreduce.output.fileoutputformat.compress.codec')

        if compress_conf and compress_conf != 'false' and codec_conf:
            args.extend(['--compression-codec', codec_conf])

        return args

    def _spark_harness_path(self):
        """Where to find the Spark harness."""
        path = mrjob_spark_harness.__file__
        if path.endswith('.pyc'):
            path = path[:-1]
        return path
