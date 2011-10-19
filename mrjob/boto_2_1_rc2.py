# Copyright (c) 2010 Spotify AB
# Copyright (c) 2010-2011 Yelp
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.
"""Code from boto 2.1rc2, copied here so that mrjob can formally depend
on a stable release of boto (in this case, 2.0).

This module will hopefully go away in mrjob v0.4.

Please don't make multiple boto_* modules; just bump the module name to
whatever version you need to work from, and re-copy the relevant code.

This is intentionally somewhat ugly and tedious; our goal is to check
the patches we need into boto as fast as we can, so that we don't need
to copy code from future versions of boto into mrjob.
"""
import types

import boto.emr.connection
from boto.emr.emrobject import RunJobFlowResponse
from boto.emr.step import JarStep


# copied in run_jobflow() and supporting functions. This supports the
# instance_groups and additional_info keyword args, which don't exist
# in boto 2.0
class EmrConnection(boto.emr.connection.EmrConnection):

    def run_jobflow(self, name, log_uri, ec2_keyname=None, availability_zone=None,
                    master_instance_type='m1.small',
                    slave_instance_type='m1.small', num_instances=1,
                    action_on_failure='TERMINATE_JOB_FLOW', keep_alive=False,
                    enable_debugging=False,
                    hadoop_version='0.20',
                    steps=[],
                    bootstrap_actions=[],
                    instance_groups=None,
                    additional_info=None):
        """
        Runs a job flow

        :type name: str
        :param name: Name of the job flow
        :type log_uri: str
        :param log_uri: URI of the S3 bucket to place logs
        :type ec2_keyname: str
        :param ec2_keyname: EC2 key used for the instances
        :type availability_zone: str
        :param availability_zone: EC2 availability zone of the cluster
        :type master_instance_type: str
        :param master_instance_type: EC2 instance type of the master
        :type slave_instance_type: str
        :param slave_instance_type: EC2 instance type of the slave nodes
        :type num_instances: int
        :param num_instances: Number of instances in the Hadoop cluster
        :type action_on_failure: str
        :param action_on_failure: Action to take if a step terminates
        :type keep_alive: bool
        :param keep_alive: Denotes whether the cluster should stay alive upon completion
        :type enable_debugging: bool
        :param enable_debugging: Denotes whether AWS console debugging should be enabled.
        :type steps: list(boto.emr.Step)
        :param steps: List of steps to add with the job
        :type bootstrap_actions: list(boto.emr.BootstrapAction)
        :param bootstrap_actions: List of bootstrap actions that run before Hadoop starts.
        :type instance_groups: list(boto.emr.InstanceGroup)
        :param instance_groups: Optional list of instance groups to use when creating
                      this job. NB: When provided, this argument supersedes
                      num_instances and master/slave_instance_type.
        :type additional_info: JSON str
        :param additional_info: A JSON string for selecting additional features
        :rtype: str
        :return: The jobflow id
        """
        params = {}
        if action_on_failure:
            params['ActionOnFailure'] = action_on_failure
        params['Name'] = name
        params['LogUri'] = log_uri

        # Common instance args
        common_params = self._build_instance_common_args(ec2_keyname,
                                                         availability_zone,
                                                         keep_alive, hadoop_version)
        params.update(common_params)

        # NB: according to the AWS API's error message, we must
        # "configure instances either using instance count, master and
        # slave instance type or instance groups but not both."
        #
        # Thus we switch here on the truthiness of instance_groups.
        if not instance_groups:
            # Instance args (the common case)
            instance_params = self._build_instance_count_and_type_args(
                                                        master_instance_type,
                                                        slave_instance_type,
                                                        num_instances)
            params.update(instance_params)
        else:
            # Instance group args (for spot instances or a heterogenous cluster)
            list_args = self._build_instance_group_list_args(instance_groups)
            instance_params = dict(
                ('Instances.%s' % k, v) for k, v in list_args.iteritems()
                )
            params.update(instance_params)

        # Debugging step from EMR API docs
        if enable_debugging:
            debugging_step = JarStep(name='Setup Hadoop Debugging',
                                     action_on_failure='TERMINATE_JOB_FLOW',
                                     main_class=None,
                                     jar=self.DebuggingJar,
                                     step_args=self.DebuggingArgs)
            steps.insert(0, debugging_step)

        # Step args
        if steps:
            step_args = [self._build_step_args(step) for step in steps]
            params.update(self._build_step_list(step_args))

        if bootstrap_actions:
            bootstrap_action_args = [self._build_bootstrap_action_args(bootstrap_action) for bootstrap_action in bootstrap_actions]
            params.update(self._build_bootstrap_action_list(bootstrap_action_args))

        if additional_info is not None:
            params['AdditionalInfo'] = additional_info

        response = self.get_object(
            'RunJobFlow', params, RunJobFlowResponse, verb='POST')
        return response.jobflowid

    def _build_instance_common_args(self, ec2_keyname, availability_zone,
                                    keep_alive, hadoop_version):
        """
        Takes a number of parameters used when starting a jobflow (as
        specified in run_jobflow() above). Returns a comparable dict for
        use in making a RunJobFlow request.
        """
        params = {
            'Instances.KeepJobFlowAliveWhenNoSteps' : str(keep_alive).lower(),
            'Instances.HadoopVersion' : hadoop_version
        }

        if ec2_keyname:
            params['Instances.Ec2KeyName'] = ec2_keyname
        if availability_zone:
            params['Instances.Placement.AvailabilityZone'] = availability_zone

        return params

    def _build_instance_count_and_type_args(self, master_instance_type,
                                            slave_instance_type, num_instances):
        """
        Takes a master instance type (string), a slave instance type
        (string), and a number of instances. Returns a comparable dict
        for use in making a RunJobFlow request.
        """
        params = {
            'Instances.MasterInstanceType' : master_instance_type,
            'Instances.SlaveInstanceType' : slave_instance_type,
            'Instances.InstanceCount' : num_instances,
            }
        return params

    def _build_instance_group_args(self, instance_group):
        """
        Takes an InstanceGroup; returns a dict that, when its keys are
        properly prefixed, can be used for describing InstanceGroups in
        RunJobFlow or AddInstanceGroups requests.
        """
        params = {
            'InstanceCount' : instance_group.num_instances,
            'InstanceRole' : instance_group.role,
            'InstanceType' : instance_group.type,
            'Name' : instance_group.name,
            'Market' : instance_group.market
        }
        if instance_group.market == 'SPOT':
            params['BidPrice'] = instance_group.bidprice
        return params

    def _build_instance_group_list_args(self, instance_groups):
        """
        Takes a list of InstanceGroups, or a single InstanceGroup. Returns
        a comparable dict for use in making a RunJobFlow or AddInstanceGroups
        request.
        """
        if type(instance_groups) != types.ListType:
            instance_groups = [instance_groups]

        params = {}
        for i, instance_group in enumerate(instance_groups):
            ig_dict = self._build_instance_group_args(instance_group)
            for key, value in ig_dict.iteritems():
                params['InstanceGroups.member.%d.%s' % (i+1, key)] = value
        return params
