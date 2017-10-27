# Copyright 2009-2017 Yelp and Contributors
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
"""Mock boto3 EMR support."""
from copy import deepcopy
from datetime import datetime

from botocore.exceptions import ClientError
from botocore.exceptions import ParamValidationError

from mrjob.aws import _DEFAULT_AWS_REGION
from mrjob.aws import _boto3_now
from mrjob.compat import map_version
from mrjob.compat import version_gte
from mrjob.conf import combine_values
from mrjob.parse import is_s3_uri
from mrjob.parse import parse_s3_uri
from mrjob.pool import _extract_tags
from mrjob.py2 import integer_types
from mrjob.py2 import string_types

from .s3 import add_mock_s3_data
from .util import MockClientMeta
from .util import MockPaginator


# what partial versions and "latest" map to, as of 2015-07-15
AMI_VERSION_ALIASES = {
    None: '1.0.0',  # API does this for old accounts
    '2.0': '2.0.6',
    '2.1': '2.1.4',
    '2.2': '2.2.4',
    '2.3': '2.3.6',
    '2.4': '2.4.11',
    '3.0': '3.0.4',
    '3.1': '3.1.4',
    '3.2': '3.2.3',
    '3.3': '3.3.2',
    '3.4': '3.4.0',
    '3.5': '3.5.0',
    '3.6': '3.6.0',
    '3.7': '3.7.0',
    '3.8': '3.8.0',
    '3.9': '3.9.0',
    '3.10': '3.10.0',
    '3.11': '3.11.0',
    'latest': '2.4.2',
}

# versions of hadoop for each AMI
AMI_HADOOP_VERSION_UPDATES = {
    '1.0.0': '0.20',
    '2.0.0': '0.20.205',
    '2.2.0': '1.0.3',
    '3.0.0': '2.2.0',
    '3.1.0': '2.4.0',
    '4.0.0': '2.6.0',
    '4.3.0': '2.7.1',
    '4.5.0': '2.7.2',
    '4.8.2': '2.7.3',
    '5.0.0': '2.7.2',
    '5.0.3': '2.7.3',
}

# does the AMI have a limit on the *total* number of steps?
# see http://docs.aws.amazon.com/emr/latest/ManagementGuide/AddMoreThan256Steps.html  # noqa
LIFETIME_STEP_LIMIT_AMI_VERSIONS = {
    '1.0.0': True,
    '2.4.8': False,
    '3.0.0': True,
    '3.1.1': False,
}

# does the given AMI version support instance fleets?
INSTANCE_FLEET_AMI_VERSIONS = {
    '4': False,
    '4.8': True,
    '5.0': False,
    '5.1': True,
}

# a cluster can't have more than this many active steps (or more than this
# many steps total
STEP_ADD_LIMIT = 256

# only the last 1000 steps are visible through the API
STEP_LIST_LIMIT = 1000

# list_clusters() only returns this many results at a time
DEFAULT_MAX_CLUSTERS_RETURNED = 50

# list_steps() only returns this many results at a time
DEFAULT_MAX_STEPS_RETURNED = 50

# need to fill in a version for non-Hadoop applications
DUMMY_APPLICATION_VERSION = '0.0.0'


# TODO: raise InvalidRequest: Missing required header for this
# request: x-amz-content-sha256 when region name is clearly invalid

class MockEMRClient(object):
    """Mock out boto3 EMR clients. This actually handles a small
    state machine that simulates EMR clusters."""

    DEFAULT_MAX_ITEMS = 50

    OPERATION_NAME_TO_RESULT_KEY = dict(
        list_bootstrap_actions='BootstrapActions',
        list_clusters='Clusters',
        list_instance_fleets='InstanceFleets',
        list_instance_groups='InstanceGroups',
        list_instances='Instances',
        list_steps='Steps',
    )

    def __init__(self,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 aws_session_token=None,
                 endpoint_url=None,
                 region_name=None,
                 mock_s3_fs=None,
                 mock_emr_clusters=None,
                 mock_emr_failures=None,
                 mock_emr_self_termination=None,
                 mock_emr_output=None,
                 max_clusters_returned=DEFAULT_MAX_CLUSTERS_RETURNED,
                 max_steps_returned=DEFAULT_MAX_STEPS_RETURNED):
        """Create a mock version boto3 EMR clients.

        By default, jobs will run to conclusion, and if their output dir
        is on S3, create a single empty output file. You can manually
        decide that some jobs will fail, or give them different output
        by setting mock_emr_failures/mock_emr_output.

        Clusters are given IDs j-MOCKCLUSTER0, j-MOCKCLUSTER1, etc.
        Step numbers are 0-indexed.

        Extra args:
        :param mock_s3_fs: a mock S3 filesystem to point to (usually you just
                            want to use an empty dictionary).
        :param mock_emr_clusters: map from cluster ID to an EMRObject, in the
                                  format returned by describe_cluster(), plus
                                 ``_bootstrapactions``, ``_instancegroups``,
                                 and ``_steps`` fields.
        :param mock_emr_failures: a set of ``(cluster ID, step_num)`` for steps
                                  that should fail.
        :param mock_emr_self_termination: a set of cluster IDs that should
                                          simulate master node termination
                                          once cluster is up
        :param mock_emr_output: a map from ``(cluster ID, step_num)`` to a
                                list of ``str``s representing file contents to
                                output when the job completes
        :type max_clusters_returned: int
        :param max_clusters_returned: the maximum number of clusters that
                                       :py:meth:`list_clusters` can return,
                                       to simulate a real limitation of EMR
        :type max_steps_returned: int
        :param max_steps_returned: the maximum number of clusters that
                                   :py:meth:`list_steps` can return,
                                   to simulate a real limitation of EMR
        :type max_days_ago: int
        :param max_days_ago: the maximum amount of days that EMR will go back
                             in time
        """
        # check this now; strs will cause problems later in Python 3
        if mock_emr_output and any(
                any(not isinstance(part, bytes) for part in parts)
                for parts in mock_emr_output.values()):
            raise TypeError('mock EMR output must be bytes')

        self.mock_s3_fs = combine_values({}, mock_s3_fs)
        self.mock_emr_clusters = combine_values({}, mock_emr_clusters)
        self.mock_emr_failures = combine_values(set(), mock_emr_failures)
        self.mock_emr_self_termination = combine_values(
            set(), mock_emr_self_termination)
        self.mock_emr_output = combine_values({}, mock_emr_output)
        self.max_clusters_returned = max_clusters_returned
        self.max_steps_returned = max_steps_returned

        region_name = region_name or _DEFAULT_AWS_REGION
        if not endpoint_url:
            if region_name == _DEFAULT_AWS_REGION:
                # not entirely sure why boto3 1.4.4 uses a different format for
                # us-east-1, but there it is. according to AWS docs, the
                # host name is elasticmapreduce.<region>.amazonaws.com.
                endpoint_url = (
                    'https://elasticmapreduce.%s.amazonaws.com' % region_name)
            else:
                endpoint_url = (
                    'https://%s.elasticmapreduce.amazonaws.com' % region_name)

        self.meta = MockClientMeta(
            endpoint_url=endpoint_url,
            region_name=region_name)

    # TODO: merge with code from MockIAMClient
    def get_paginator(self, operation_name):
        return MockPaginator(
            getattr(self, operation_name),
            self.OPERATION_NAME_TO_RESULT_KEY[operation_name],
            self.DEFAULT_MAX_ITEMS)

    def run_job_flow(self, **kwargs):
        # going to pop params from kwargs as we process then, and raise
        # NotImplementedError at the end if any params are left
        now = kwargs.pop('_Now', _boto3_now())

        # our newly created cluster, as described by describe_cluster(), plus:
        #
        # _BootstrapActions: as described by list_bootstrap_actions()
        # _InstanceFleets: as described by list_instance_fleets()
        # _InstanceGroups: as described by list_instance_groups()
        # _Steps: as decribed by list_steps(), but not reversed
        cluster = dict(
            _BootstrapActions=[],
            _InstanceFleets=[],
            _InstanceGroups=[],
            _Steps=[],
            Applications=[],
            AutoTerminate=True,
            Configurations=[],
            Ec2InstanceAttributes=dict(
                EmrManagedMasterSecurityGroup='sg-mockmaster',
                EmrManagedSlaveSecurityGroup='sg-mockslave',
                IamInstanceProfile='',
            ),
            Id='j-MOCKCLUSTER%d' % len(self.mock_emr_clusters),
            Name='',
            NormalizedInstanceHours=0,
            ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
            ServiceRole='',
            Status=dict(
                State='STARTING',
                StateChangeReason={},
                Timeline=dict(CreationDateTime=now),
            ),
            Tags=[],
            TerminationProtected=False,
            VisibleToAllUsers=False,
        )

        def _error(message):
            return _ValidationException('RunJobFlow', message)

        # Name (required)
        _validate_param(kwargs, 'Name', string_types)
        cluster['Name'] = kwargs.pop('Name')

        # LogUri
        if 'LogUri' in kwargs:
            _validate_param(kwargs, 'LogUri', string_types)
            cluster['LogUri'] = kwargs.pop('LogUri')

        # JobFlowRole and ServiceRole (required)
        _validate_param(kwargs, 'JobFlowRole', string_types)
        cluster['Ec2InstanceAttributes']['IamInstanceProfile'] = kwargs.pop(
            'JobFlowRole')

        if 'ServiceRole' not in kwargs:  # required by API, not boto3
            raise _error('ServiceRole is required for creating cluster.')
        _validate_param(kwargs, 'ServiceRole', string_types)
        cluster['ServiceRole'] = kwargs.pop('ServiceRole')

        # AutoScalingRole
        if 'AutoScalingRole' in kwargs:
            _validate_param(kwargs, 'AutoScalingRole', string_types)
            cluster['AutoScalingRole'] = kwargs.pop('AutoScalingRole')

        # AmiVersion and ReleaseLabel
        for version_param in ('AmiVersion', 'ReleaseLabel'):
            if version_param in kwargs:
                _validate_param(kwargs, version_param, string_types)

        if 'AmiVersion' in kwargs:
            if 'ReleaseLabel' in kwargs:
                raise _error(
                    'Only one AMI version and release label may be specified.'
                    ' Provided AMI: %s, release label: %s.' % (
                        kwargs['AmiVersion'], kwargs['ReleaseLabel']))

            AmiVersion = kwargs.pop('AmiVersion')

            running_ami_version = AMI_VERSION_ALIASES.get(
                AmiVersion, AmiVersion)

            if version_gte(running_ami_version, '4'):
                raise _error('The supplied ami version is invalid.')
            elif not version_gte(running_ami_version, '2'):
                raise _error(
                    'Job flow role is not compatible with the supplied'
                    ' AMI version')

            cluster['RequestedAmiVersion'] = AmiVersion
            cluster['RunningAmiVersion'] = running_ami_version

        elif 'ReleaseLabel' in kwargs:
            ReleaseLabel = kwargs.pop('ReleaseLabel')
            running_ami_version = ReleaseLabel.lstrip('emr-')

            if not version_gte(running_ami_version, '4'):
                raise _error('The supplied release label is invalid: %s.' %
                             ReleaseLabel)

            cluster['ReleaseLabel'] = ReleaseLabel
        else:
            # note: you can't actually set Hadoop version through boto3
            raise _error(
                'Must specify exactly one of the following:'
                ' release label, AMI version, or Hadoop version.')

        # Applications
        hadoop_version = map_version(
            running_ami_version, AMI_HADOOP_VERSION_UPDATES)

        if version_gte(running_ami_version, '4'):
            if kwargs.get('SupportedProducts'):
                raise _error(
                    'Cannot specify supported products when release label is'
                    ' used. Specify applications instead.')

            application_names = set(
                a['Name'] for a in kwargs.pop('Applications', []))

            # if Applications is set but doesn't include Hadoop, the
            # cluster description won't either! (Even though Hadoop is
            # in fact installed.)
            if not application_names:
                application_names = set(['Hadoop'])

            for app_name in sorted(application_names):
                if app_name == 'Hadoop':
                    version = hadoop_version
                else:
                    version = DUMMY_APPLICATION_VERSION

                cluster['Applications'].append(
                    dict(Name=app_name, Version=version))
        else:
            if kwargs.get('Applications'):
                raise _error(
                    'Cannot specify applications when AMI version is used.'
                    ' Specify supported products or new supported products'
                    ' instead.')

            # 'hadoop' is lowercase if AmiVersion specified
            cluster['Applications'].append(
                dict(Name='hadoop', Version=hadoop_version))

            if kwargs.get('SupportedProducts'):
                _validate_param(kwargs, 'SupportedProducts', (list, tuple))
                for product in kwargs.pop('SupportedProducts'):
                    cluster['Applications'].append(dict(Name=product))

        # Configurations
        if 'Configurations' in kwargs:
            _validate_param(kwargs, 'Configurations', (list, tuple))

            if kwargs['Configurations'] and not version_gte(
                    running_ami_version, '4'):
                raise _ValidationException(
                    'RunJobFlow',
                    'Cannot specify configurations when AMI version is used.')

            cluster['Configurations'] = _normalized_configurations(
                kwargs.pop('Configurations'))

        # VisibleToAllUsers
        if 'VisibleToAllUsers' in kwargs:
            _validate_param(kwargs, 'VisibleToAllUsers', bool)
            cluster['VisibleToAllUsers'] = kwargs.pop('VisibleToAllUsers')

        # pass BootstrapActions off to helper
        if 'BootstrapActions' in kwargs:
            self._add_bootstrap_actions(
                'RunJobFlow', kwargs.pop('BootstrapActions'), cluster)

        # pass Instances (required) off to helper
        _validate_param(kwargs, 'Instances')
        self._add_instances('RunJobFlow', kwargs.pop('Instances'), cluster,
                            now=now)

        # pass Steps off to helper
        if 'Steps' in kwargs:
            self._add_steps('RunJobFlow', kwargs.pop('Steps'), cluster)

        # pass Tags off to helper
        if 'Tags' in kwargs:
            self._add_tags('RunJobFlow', kwargs.pop('Tags'), cluster)

        # save AdditionalInfo
        if 'AdditionalInfo' in kwargs:
            cluster['_AdditionalInfo'] = kwargs.pop('AdditionalInfo')

        # catch extra params
        if kwargs:
            raise NotImplementedError(
                'mock RunJobFlow does not support these parameters: %s' %
                ', '.join(sorted(kwargs)))

        self.mock_emr_clusters[cluster['Id']] = cluster

        return dict(JobFlowId=cluster['Id'])

    # helper methods for run_job_flow()

    def _add_bootstrap_actions(
            self, operation_name, BootstrapActions, cluster):
        """Handle BootstrapActions param from run_job_flow().

        (there isn't any other way to add bootstrap actions)
        """
        _validate_param_type(BootstrapActions, (list, tuple))

        operation_name  # currently unused, quiet pyflakes

        new_actions = []  # don't update _BootstrapActions if there's an error

        for ba in BootstrapActions:
            _validate_param_type(ba, dict)
            _validate_param(ba, 'Name', string_types)
            _validate_param(ba, 'ScriptBootstrapAction', dict)
            _validate_param(ba['ScriptBootstrapAction'], 'Path', string_types)

            args = []
            if 'Args' in ba['ScriptBootstrapAction']:
                _validate_param(
                    ba['ScriptBootstrapAction'], 'Args', (list, tuple))
                for arg in ba['ScriptBootstrapAction']['Args']:
                    _validate_param_type(arg, string_types)
                    args.append(arg)

            new_actions.append(dict(
                Name=ba['Name'],
                ScriptPath=ba['ScriptBootstrapAction']['Path'],
                Args=args))

        cluster['_BootstrapActions'].extend(new_actions)

    def _add_instances(self, operation_name, Instances, cluster, now=None):
        """Handle Instances param from run_job_flow()"""
        if now is None:
            now = _boto3_now()

        _validate_param_type(Instances, dict)

        Instances = dict(Instances)  # going to pop params from Instances

        def _error(message):
            return _ValidationException(operation_name, message)

        # Ec2KeyName
        if 'Ec2KeyName' in Instances:
            _validate_param(Instances, 'Ec2KeyName', string_types)
            cluster['Ec2InstanceAttributes']['Ec2KeyName'] = Instances.pop(
                'Ec2KeyName')

        # Ec2SubnetId
        if 'Ec2SubnetId' in Instances:
            if 'Ec2SubnetIds' in Instances:
                # TODO: get actual error message
                raise _error('Ec2SubnetId and Ec2SubnetIds together')

            _validate_param(Instances, 'Ec2SubnetId', string_types)
            cluster['Ec2InstanceAttributes']['Ec2SubnetId'] = (
                Instances.pop('Ec2SubnetId'))

        # Ec2SubnetIds
        if Instances.get('Ec2SubnetIds'):
            if 'InstanceFleets' not in Instances:
                raise _error('Only one subnet may be specified for clusters'
                             ' configured with instance groups or instance'
                             ' count, master and slave instance type. Revise'
                             ' the configuration and resubmit.')

            _validate_param(Instances, 'Ec2SubnetIds', list)
            Ec2SubnetIds = Instances.pop('Ec2SubnetIds')

            for subnet_id in Ec2SubnetIds:
                _validate_param_type(subnet_id, string_types)
                # arbitrarily choose last subnet ID
                cluster['Ec2InstanceAttributes']['Ec2SubnetId'] = subnet_id

        # KeepJobFlowAliveWhenNoSteps
        if 'KeepJobFlowAliveWhenNoSteps' in Instances:
            _validate_param(Instances, 'KeepJobFlowAliveWhenNoSteps', bool)
            cluster['AutoTerminate'] = (
                not Instances.pop('KeepJobFlowAliveWhenNoSteps'))

        # Placement (availability zone)
        if 'Placement' in Instances:
            _validate_param(Instances, 'Placement', dict)
            Placement = Instances.pop('Placement')

            # mock_boto3 doesn't support the 'AvailabilityZones' param
            _validate_param(Placement, 'AvailabilityZone', string_types)
            cluster['Ec2InstanceAttributes']['Ec2AvailabilityZone'] = (
                Placement['AvailabilityZone'])

        # don't allow mixing and matching of instance def types
        num_config_types = sum(
            any(x in Instances for x in fields)
            for fields in [
                {'InstanceFleets'},
                {'InstanceGroups'},
                {'InstanceCount', 'MasterInstanceType', 'SlaveInstanceType'}])
        if num_config_types > 1:
            raise _error(
                'Please configure instances using one and only one of the'
                ' following: instance groups; instance fleets; instance'
                ' count, master and slave instance type.')

        if 'InstanceGroups' in Instances:
            self._add_instance_groups(
                operation_name, Instances.pop('InstanceGroups'), cluster)
        elif 'InstanceFleets' in Instances:
            self._add_instance_fleets(
                operation_name, Instances.pop('InstanceFleets'), cluster)
        else:
            # build our own instance groups
            instance_groups = []

            instance_count = Instances.pop('InstanceCount', 0)
            _validate_param_type(instance_count, integer_types)

            # note: boto3 actually lets 'null' fall through to the API here
            _validate_param(Instances, 'MasterInstanceType', string_types)
            instance_groups.append(dict(
                InstanceRole='MASTER',
                InstanceType=Instances.pop('MasterInstanceType'),
                InstanceCount=1))

            if 'SlaveInstanceType' in Instances:
                SlaveInstanceType = Instances.pop('SlaveInstanceType')
                _validate_param_type(SlaveInstanceType, string_types)

                # don't create a group with no instances!
                if instance_count > 1:
                    instance_groups.append(dict(
                        InstanceRole='CORE',
                        InstanceType=SlaveInstanceType,
                        InstanceCount=instance_count - 1))

            self._add_instance_groups(
                operation_name, instance_groups, cluster, now=now)

        if Instances:
            raise NotImplementedError(
                'mock %s does not support these parameters: %s' % (
                    operation_name,
                    ', '.join('Instances.%s' % k for k in sorted(Instances))))

    def _add_instance_fleets(self, operation_name, InstanceFleets, cluster,
                             now=None):
        """Add instance fleets from *InstanceFleets* to the mock cluster
        *cluster*. This is just a helper for :py:meth:`add_instances`;
        there is no ``AddInstanceFleets`` operation.
        """
        _validate_param_type(InstanceFleets, (list, tuple))

        if now is None:
            now = _boto3_now()

        def _error(message):
            return _ValidationException(operation_name, message)

        # only allowed for AMI 4.8.0+ and 5.1.0+
        if not (cluster.get('ReleaseLabel') and
                map_version(cluster['ReleaseLabel'].lstrip('emr-'),
                            INSTANCE_FLEET_AMI_VERSIONS)):
            raise _error('Instance fleets are not available for the release.')

        if cluster.get('_InstanceGroups') or cluster.get('_InstanceFleets'):
            raise ValueError(
                "cluster already has instance groups or instance fleets")

        new_fleets = []  # don't update _InstanceFleets if there's an error

        roles = set()  # roles already handled

        for i, InstanceFleet in enumerate(InstanceFleets):
            _validate_param_type(InstanceFleet, dict)
            InstanceFleet = dict(InstanceFleet)

            fleet = dict(
                Id='if-FAKE',
                InstanceFleetType='',
                InstanceTypeSpecifications=[],
                ProvisionedOnDemandCapacity=0,
                ProvisionedSpotCapacity=0,
                Status=dict(
                    State='PROVISIONING',
                    StateChangeReason=dict(Message=''),
                    Timeline=dict(CreationDateTime=now),
                ),
                TargetOnDemandCapacity=0,
                TargetSpotCapacity=0,
            )

            # InstanceFleetType (required)
            _validate_param(InstanceFleet, 'InstanceFleetType',
                            ['MASTER', 'CORE', 'TASK'])
            role = InstanceFleet.pop('InstanceFleetType')

            # check for duplicate roles
            if role in roles:
                raise _error(
                    'Multiple %s instance groups supplied, you'
                    ' must specify exactly one %s instance group' %
                    (role.lower(), role.lower()))
            roles.add(role)

            fleet['InstanceFleetType'] = role

            # Name
            if 'Name' in InstanceFleet:
                _validate_param(InstanceFleet, 'Name', string_types)
                fleet['Name'] = InstanceFleet.pop['Name']

            # InstanceTypeConfigs
            _validate_param(InstanceFleet, 'InstanceTypeConfigs', list)
            fleet['InstanceTypeSpecifications'] = (
                self._instance_type_configs_to_specs(
                    operation_name,
                    InstanceFleet.pop('InstanceTypeConfigs'),
                    fleet.get('Name'), fleet['InstanceFleetType']))

            # LaunchSpecifications
            if 'LaunchSpecifications' in InstanceFleet:
                _validate_param(InstanceFleet, 'LaunchSpecifications', dict)
                LaunchSpecifications = InstanceFleet.pop(
                    'LaunchSpecifications')

                _validate_param(LaunchSpecifications, 'SpotSpecification')
                SpotSpecification = LaunchSpecifications['SpotSpecification']

                _validate_param(
                    SpotSpecification, 'TimeoutAction', string_types)
                _validate_param_enum(
                    SpotSpecification['TimeoutAction'],
                    ['SWITCH_TO_ON_DEMAND', 'TERMINATE_CLUSTER'])
                _validate_param(
                    SpotSpecification, 'TimeoutDurationMinutes', int)

                fleet['LaunchSpecifications'] = deepcopy(LaunchSpecifications)

            # target capacity
            for target_field in ('TargetOnDemandCapacity',
                                 'TargetSpotCapacity'):
                if target_field in InstanceFleet:
                    _validate_param(InstanceFleet, target_field, int)
                    fleet[target_field] = InstanceFleet.pop(target_field)

            target_capacity = (fleet['TargetOnDemandCapacity'] +
                               fleet['TargetSpotCapacity'])

            if role == 'MASTER' and target_capacity != 1:
                raise _error('A master instance fleet can only have a target'
                             ' capacity of 1. Revise the configuration and'
                             ' resubmit.')
            elif target_capacity < 1:
                raise _error('The instance fleet (%s) should have a value of'
                             ' one or greater for either'
                             ' targetOnDemandCapacity or targetSpotCapacity.' %
                             fleet.get('Name', 'null'))

            if InstanceFleet:
                raise NotImplementedError(
                    'mock_boto3 does not support these InstanceFleet'
                    ' params: %s' % ', '.join(sorted(InstanceFleet)))

            new_fleets.append(fleet)

        # TASK roles require CORE roles (to host HDFS)
        if 'TASK' in roles and 'CORE' not in roles:
            raise _error(
                'Clusters with task instance fleets must also define core'
                ' instance fleets.')

        # MASTER role is required
        if 'MASTER' not in roles:
            raise _error('No master instance fleets were supplied; you must'
                         ' specify exactly one master instance fleet. Revise'
                         ' the configuration and resubmit.')

        cluster['_InstanceFleets'].extend(new_fleets)

        cluster['InstanceCollectionType'] = 'INSTANCE_FLEET'

    def _instance_type_configs_to_specs(
            self, operation_name,
            InstanceTypeConfigs, Name, InstanceFleetType):
        """Validate InstanceTypeConfigs from fleet request, and convert
        to InstanceTypeSpecifications (from ListInstanceFleets)."""
        specs = []

        instance_types = set()  # so we don't get duplicates

        for InstanceTypeConfig in InstanceTypeConfigs:
            _validate_param(InstanceTypeConfig, 'InstanceType', string_types)
            InstanceType = InstanceTypeConfig['InstanceType']

            if InstanceType in instance_types:
                raise _ValidationException(
                    operation_name,
                    'The instance fleet: %s contains duplicate instance types'
                    ' [%s]. Revise the configuration and resubmit.' % (
                        Name or 'null', InstanceType))

            specs.append(
                self._instance_type_config_to_spec(
                    operation_name,
                    InstanceTypeConfig, Name, InstanceFleetType))

        return specs

    def _instance_type_config_to_spec(
            self, operation_name, InstanceTypeConfig, Name, InstanceFleetType):

        def _error(message):
            return _ValidationException(operation_name, message)

        spec = {}  # the result

        # make a copy so we can pop out fields as we
        InstanceTypeConfig = dict(InstanceTypeConfig)

        # InstanceType
        _validate_param(InstanceTypeConfig, 'InstanceType', string_types)
        spec['InstanceType'] = InstanceTypeConfig.pop('InstanceType')

        # WeightedCapacity
        if 'WeightedCapacity' not in InstanceTypeConfig:
            WeightedCapacity = 1  # the default
        else:
            _validate_param(InstanceTypeConfig, 'WeightedCapacity', int)
            WeightedCapacity = InstanceTypeConfig.pop('WeightedCapacity')
            if InstanceFleetType == 'MASTER' and WeightedCapacity != 1:
                raise _error('All instance types in the master instance fleet'
                             ' must have weighted capacity as 1. Revise the'
                             ' configuration and resubmit.')
            elif WeightedCapacity < 1:
                raise _error('Weighted capacity cannot be zero. Revise the'
                             ' configuration and resubmit.')

        spec['WeightedCapacity'] = WeightedCapacity

        # BidPrice
        if 'BidPrice' in InstanceTypeConfig:

            if 'BidPriceAsPercentageOfOnDemandPrice' in InstanceTypeConfig:
                raise _error('Specify at most one of bidPrice or'
                             ' bidPriceAsPercentageOfOnDemandPrice value for'
                             ' the Spot Instance fleet : %s request.' % (
                                 Name or 'null'))

            _validate_param(InstanceTypeConfig, 'BidPrice', string_types)
            BidPrice = InstanceTypeConfig.pop('BidPrice')

            try:
                if not float(BidPrice) > 0:
                        raise _error('The bid price is negative or zero.')
            except (TypeError, ValueError):
                raise _error(
                    'The bid price supplied for an instance fleet is'
                    ' invalid')

            if '.' in BidPrice and len(BidPrice.split('.', 1)[1]) > 3:
                    raise _error('No more than 3 digits are allowed after'
                                 ' decimal place in bid price')

            spec['BidPrice'] = BidPrice

        elif 'BidPriceAsPercentageOfOnDemandPrice' in InstanceTypeConfig:
            _validate_param(InstanceTypeConfig,
                            'BidPriceAsPercentageOfOnDemandPrice',
                            (int, float))
            # boto3 disallows negative prices, API doesn't seem to care
            spec['BidPriceAsPercentageOfOnDemandPrice'] = float(
                InstanceTypeConfig.pop('BidPriceAsPercentageOfOnDemandPrice'))

        else:
            spec['BidPriceAsPercentageOfOnDemandPrice'] = 100.0

        # EbsConfiguration
        spec['EbsBlockDevices'] = []

        if 'EbsConfiguration' in InstanceTypeConfig:
            _validate_param(InstanceTypeConfig, 'EbsConfiguration', dict)
            EbsConfiguration = InstanceTypeConfig.pop('EbsConfiguration')

            # EbsOptimized
            if 'EbsOptimized' in EbsConfiguration:
                _validate_param(EbsConfiguration, 'EbsOptimized', bool)
                if EbsConfiguration['EbsOptimized']:
                    spec['EbsOptimized'] = True

            # EbsBlockDeviceConfigs
            if 'EbsBlockDeviceConfigs' in EbsConfiguration:
                _validate_param(EbsConfiguration,
                                'EbsBlockDeviceConfigs', list)

                spec['EbsBlockDevices'].extend(
                    self._ebs_block_device_configs_to_block_devices(
                        operation_name,
                        EbsConfiguration['EbsBlockDeviceConfigs']))

        if InstanceTypeConfig:
            raise NotImplementedError(
                'mock_boto3 does not support these InstanceTypeConfig'
                ' params: %s' % ', '.join(sorted(InstanceTypeConfig)))

        return spec

    def _ebs_block_device_configs_to_block_devices(
            self, operation_name, EbsBlockDeviceConfigs):

        devices = []

        for EbsBlockDeviceConfig in EbsBlockDeviceConfigs:
            _validate_param(
                EbsBlockDeviceConfig, 'VolumeSpecification', dict)
            VolumeSpecification = (
                EbsBlockDeviceConfig['VolumeSpecification'])

            _validate_param(VolumeSpecification, 'SizeInGB', int)
            _validate_param(VolumeSpecification, 'VolumeType',
                            string_types)

            if 'Iops' in VolumeSpecification:
                _validate_param(VolumeSpecification, 'Iops', int)
                Iops = VolumeSpecification['Iops']

                if VolumeSpecification['VolumeType'] != 'io1':
                    raise _ValidationException(
                        operation_name,
                        'IOPS setting is not supported for volume'
                        ' type')

                if Iops < 100:
                    raise _ValidationException(
                        operation_name,
                        'The iops is less than minimum value(100).'
                    )

                if Iops > 20000:
                    raise _ValidationException(
                        operation_name,
                        'The iops is more than maximum'
                        ' value(20000).')
            elif VolumeSpecification['VolumeType'] == 'io1':
                raise _ValidationException(
                    operation_name,
                    'IOPS setting is required for volume type.')

            if 'VolumesPerInstance' in EbsBlockDeviceConfig:
                _validate_param(EbsBlockDeviceConfig,
                                'VolumesPerInstance', int)
                VolumesPerInstance = EbsBlockDeviceConfig[
                    'VolumesPerInstance']
            else:
                VolumesPerInstance = 1

            for _ in range(VolumesPerInstance):
                # /dev/sdc, /dev/sdd, etc.
                device = 'dev/sd' + chr(
                    ord('c') + len(devices))
                devices.append(dict(
                    Device=device,
                    VolumeSpecification=dict(VolumeSpecification)))

        return devices

    def _add_instance_groups(self, operation_name, InstanceGroups, cluster,
                             now=None):
        """Add instance groups from *InstanceGroups* to the mock
        cluster *cluster*.
        """
        _validate_param_type(InstanceGroups, (list, tuple))

        def _error(message):
            return _ValidationException(operation_name, message)

        if now is None:
            now = _boto3_now()

        # currently, this is just a helper method for run_job_flow()
        if cluster.get('_InstanceGroups') or cluster.get('_InstanceFleets'):
            raise NotImplementedError(
                "mock_boto3 doesn't support adding instance groups")

        new_igs = []  # don't update _InstanceGroups if there's an error

        roles = set()  # roles already handled

        for i, InstanceGroup in enumerate(InstanceGroups):
            _validate_param_type(InstanceGroup, dict)
            InstanceGroup = dict(InstanceGroup)

            # our new mock instance group
            ig = dict(
                Configurations=[],
                EbsBlockDevices=[],
                Id='ig-FAKE',
                InstanceGroupType='',
                Market='ON_DEMAND',
                RequestedInstanceCount=0,
                RunningInstanceCount=0,
                ShrinkPolicy={},
                Status=dict(
                    State='PROVISIONING',
                    StateChangeReason=dict(Message=''),
                    Timeline=dict(CreationDateTime=now),
                ),
            )

            # InstanceRole (required)
            _validate_param(InstanceGroup, 'InstanceRole',
                            ['MASTER', 'CORE', 'TASK'])
            role = InstanceGroup.pop('InstanceRole')

            # check for duplicate roles
            if role in roles:
                raise _error(
                    'Multiple %s instance groups supplied, you'
                    ' must specify exactly one %s instance group' %
                    (role.lower(), role.lower()))
            roles.add(role)

            ig['InstanceGroupType'] = role

            # InstanceType (required)
            _validate_param(InstanceGroup, 'InstanceType', string_types)

            # 3.x AMIs (but not 4.x, etc.) reject m1.small explicitly
            if (InstanceGroup.get('InstanceType') == 'm1.small' and
                    cluster.get('RunningAmiVersion', '').startswith('3.')):
                raise _error(
                    'm1.small instance type is not supported with AMI version'
                    ' %s.' % cluster['RunningAmiVersion'])

            ig['InstanceType'] = InstanceGroup.pop('InstanceType')

            # InstanceCount (required)
            _validate_param(InstanceGroup, 'InstanceCount', integer_types)
            InstanceCount = InstanceGroup.pop('InstanceCount')
            if InstanceCount < 1:
                raise _error(
                    'An instance group must have at least one instance')

            if role == 'MASTER' and InstanceCount != 1:
                raise _error(
                    'A master instance group must specify a single instance')
            ig['RequestedInstanceCount'] = InstanceCount

            # Name
            if 'Name' in InstanceGroup:
                _validate_param(InstanceGroup, 'Name', string_types)
                ig['Name'] = InstanceGroup.pop('Name')

            # Market (default set above)
            if 'Market' in InstanceGroup:
                _validate_param(InstanceGroup, 'Market', string_types)
                if InstanceGroup['Market'] not in ('ON_DEMAND', 'SPOT'):
                    raise _error(
                        "1 validation error detected: value '%s' at"
                        " 'instances.instanceGroups.%d.member.market' failed"
                        " to satify constraint: Member must satisfy enum value"
                        " set: [SPOT, ON_DEMAND]" % (
                            InstanceGroup['Market'], i + 1))
                ig['Market'] = InstanceGroup.pop('Market')

            # BidPrice
            if 'BidPrice' in InstanceGroup:
                # not float, surprisingly
                _validate_param(InstanceGroup, 'BidPrice', string_types)

                if ig['Market'] != 'SPOT':
                    raise _error('Attempted to set bid price for on demand'
                                 ' instance group.')

                # simulate bid price validation
                BidPrice = InstanceGroup.pop('BidPrice')
                try:
                    if not float(BidPrice) > 0:
                        raise _error('The bid price is negative or zero.')
                except (TypeError, ValueError):
                    raise _error(
                        'The bid price supplied for an instance group is'
                        ' invalid')

                if '.' in BidPrice and len(BidPrice.split('.', 1)[1]) > 3:
                    raise _error('No more than 3 digits are allowed after'
                                 ' decimal place in bid price')

                ig['BidPrice'] = BidPrice

            # EbsConfiguration
            if 'EbsConfiguration' in InstanceGroup:
                EbsConfiguration = InstanceGroup.pop('EbsConfiguration')

                if 'EbsOptimized' in EbsConfiguration:
                    _validate_param(EbsConfiguration, 'EbsOptimized', bool)
                    if EbsConfiguration['EbsOptimized']:
                        ig['EbsOptimized'] = True

                if 'EbsBlockDeviceConfigs' in EbsConfiguration:
                    _validate_param(
                        EbsConfiguration, 'EbsBlockDeviceConfigs', list)

                    ig['EbsBlockDevices'].extend(
                        self._ebs_block_device_configs_to_block_devices(
                            operation_name,
                            EbsConfiguration['EbsBlockDeviceConfigs']))

            if InstanceGroup:
                raise NotImplementedError(
                    'mock_boto3 does not support these InstanceGroup'
                    ' params: %s' % ', '.join(sorted(InstanceGroup)))

            new_igs.append(ig)

        # TASK roles require CORE roles (to host HDFS)
        if 'TASK' in roles and 'CORE' not in roles:
            raise _error(
                'Clusters with task nodes must also define core nodes.')

        # MASTER role is required
        if 'MASTER' not in roles:
            raise _error('Zero master instance groups supplied, you must'
                         ' specify exactly one master instance group')

        cluster['_InstanceGroups'].extend(new_igs)

        cluster['InstanceCollectionType'] = 'INSTANCE_GROUP'

    def _add_steps(self, operation_name, Steps, cluster, now=None):
        if now is None:
            now = _boto3_now()

        _validate_param_type(Steps, (list, tuple))

        # only active job flows allowed
        if cluster['Status']['State'].startswith('TERMINAT'):
            raise _ValidationException(
                operation_name,
                'A job flow that is shutting down, terminated, or finished'
                ' may not be modified.')

        # no more than 256 steps allowed
        if cluster.get('RunningAmiVersion') and map_version(
                cluster['RunningAmiVersion'],
                LIFETIME_STEP_LIMIT_AMI_VERSIONS):
            # for very old AMIs, *all* steps count
            if len(cluster['_Steps']) + len(Steps) > STEP_ADD_LIMIT:
                raise _ValidationException(
                    operation_name,
                    'Maximum number of steps for job flow exceeded')
        else:
            # otherwise, only active and pending steps count
            num_active_steps = sum(
                1 for step in cluster['_Steps']
                if step['Status']['State'] in (
                    'PENDING', 'PENDING_CANCELLED', 'RUNNING'))

            if num_active_steps + len(Steps) > STEP_ADD_LIMIT:
                raise _ValidationException(
                    operation_name,
                    "Maximum number of active steps(State = 'Running',"
                    " 'Pending' or 'Cancel_Pending') for cluster exceeded.")

        new_steps = []

        for i, Step in enumerate(Steps):
            Step = dict(Step)

            new_step = dict(
                ActionOnFailure='TERMINATE_CLUSTER',
                Config=dict(
                    Args=[],
                    Jar={},
                    Properties={},
                ),
                Id='s-MOCKSTEP%d' % (len(cluster['_Steps']) + i),
                Name='',
                Status=dict(
                    State='PENDING',
                    StateChangeReason={},
                    Timeline=dict(CreationDateTime=now),
                ),
            )

            # Name (required)
            _validate_param(Step, 'Name', string_types)
            new_step['Name'] = Step.pop('Name')

            # ActionOnFailure
            if 'ActionOnFailure' in Step:
                _validate_param_enum(
                    Step['ActionOnFailure'],
                    ['CANCEL_AND_WAIT', 'CONTINUE',
                     'TERMINATE_JOB_FLOW', 'TERMINATE_CLUSTER'])

                new_step['ActionOnFailure'] = Step.pop('ActionOnFailure')

            # HadoopJarStep (required)
            _validate_param(Step, 'HadoopJarStep', dict)
            HadoopJarStep = dict(Step.pop('HadoopJarStep'))

            _validate_param(HadoopJarStep, 'Jar', string_types)
            new_step['Config']['Jar'] = HadoopJarStep.pop('Jar')

            if 'Args' in HadoopJarStep:
                Args = HadoopJarStep.pop('Args')
                _validate_param_type(Args, (list, tuple))
                for arg in Args:
                    _validate_param_type(arg, string_types)
                new_step['Config']['Args'].extend(Args)

            if 'MainClass' in HadoopJarStep:
                _validate_param(HadoopJarStep, 'MainClass', string_types)
                new_step['Config']['MainClass'] = HadoopJarStep.pop(
                    'MainClass')

            # we don't currently support Properties
            if HadoopJarStep:
                raise NotImplementedError(
                    "mock_boto3 doesn't support"
                    " these HadoopJarStep params: %s" %
                    ', '.join(sorted(HadoopJarStep)))

            if Step:
                raise NotImplementedError(
                    "mock_boto3 doesn't support these step params: %s" %
                    ', '.join(sorted(Step)))

            new_steps.append(new_step)

        cluster['_Steps'].extend(new_steps)

        # add_job_flow_steps() needs to return step IDs
        return [new_step['Id'] for new_step in new_steps]

    def _add_tags(self, operation_name, Tags, cluster):
        _validate_param_type(Tags, (list, tuple))

        new_tags = {}

        for Tag in Tags:
            _validate_param_type(Tag, dict)
            if set(Tag) > set(['Key', 'Value']):
                raise ParamValidationError(report='Unknown parameter in Tags')

            Key = Tag.get('Key')
            if not Key or not 1 <= len(Key) <= 128:
                raise _InvalidRequestException(
                    operation_name,
                    "Invalid tag key: '%s'. Tag keys must be between 1 and 128"
                    " characters in length." %
                    ('null' if Key is None else Key))

            Value = Tag.get('Value') or ''
            if not 0 <= len(Value) <= 256:
                raise _InvalidRequestException(
                    operation_name,
                    "Invalid tag value: '%s'. Tag values must be between 1 and"
                    " 128 characters in length." % Value)

            new_tags[Key] = Value

        tags_dict = _extract_tags(cluster)
        tags_dict.update(new_tags)

        cluster['Tags'] = [
            dict(Key=k, Value=v) for k, v in sorted(tags_dict.items())]

    def _get_mock_cluster(self, operation_name, cluster_id):
        """Get the mock cluster with the given ID, or raise
        an InvalidRequestException.
        """
        _validate_param_type(cluster_id, string_types)

        if cluster_id not in self.mock_emr_clusters:
            # error type and message depends on whether we call them
            # "job flows" or clusters
            if 'JobFlow' in operation_name:
                raise _ValidationException(
                    operation_name, 'Specified job flow ID not valid')
            else:
                raise _InvalidRequestException(
                    operation_name, 'Cluster id %r is not valid.' % cluster_id)

        return self.mock_emr_clusters[cluster_id]

    def add_job_flow_steps(self, JobFlowId, Steps):
        cluster = self._get_mock_cluster('AddJobFlowSteps', JobFlowId)

        step_ids = self._add_steps('AddJobFlowSteps', Steps, cluster)

        return dict(StepIds=step_ids)

    def add_tags(self, ResourceId, Tags):
        """Simulate successful creation of new metadata tags for the specified
        resource id.
        """
        _validate_param_type(ResourceId, string_types)
        _validate_param_type(Tags, (list, tuple))

        cluster = self._get_mock_cluster('AddTags', ResourceId)

        if cluster['Status']['State'].startswith('TERMINATED'):
            raise _InvalidRequestException(
                'AddTags', 'Tags cannot be modified on terminated clusters.')

        self._add_tags('AddTags', Tags, cluster)

    def describe_cluster(self, ClusterId):
        _validate_param_type(ClusterId, string_types)
        cluster = self._get_mock_cluster('DescribeCluster', ClusterId)

        if cluster['Status']['State'] == 'TERMINATING':
            # simulate progress, to support
            # _wait_for_logs_on_s3()
            self._simulate_progress(ClusterId)

        # hide _ fields, don't let user mess with mock cluster
        cluster = deepcopy(_strip_hidden(cluster))

        return dict(Cluster=cluster)

    def describe_step(self, ClusterId, StepId):
        # simulate progress, to support _wait_for_steps_to_complete()
        self._simulate_progress(ClusterId)

        steps = self._list_steps('DescribeStep', ClusterId, StepIds=[StepId])
        return dict(Step=steps[0])

    def list_bootstrap_actions(self, ClusterId):
        cluster = self._get_mock_cluster('ListBootstrapActions', ClusterId)

        return dict(BootstrapActions=deepcopy(cluster['_BootstrapActions']))

    def list_clusters(self, CreatedAfter=None, CreatedBefore=None,
                      ClusterStates=None):
        if CreatedAfter:
            _validate_param_type(CreatedAfter, datetime)

        if CreatedBefore:
            _validate_param_type(CreatedBefore, datetime)

        if ClusterStates:
            _validate_param_type(ClusterStates, (list, tuple))
            for cs in ClusterStates:
                _validate_param_enum(
                    cs,
                    ['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING',
                     'TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS'])

        # summaries of cluster state, to return
        cluster_summaries = []

        def created(cluster):
            return cluster['Status']['Timeline']['CreationDateTime']

        # put clusters in chronological order
        clusters = sorted(self.mock_emr_clusters.values(), key=created)

        # list_clusters starts with newest cluster
        for cluster in reversed(clusters):
            if CreatedAfter and created(cluster) < CreatedAfter:
                continue

            if CreatedBefore and created(cluster) > CreatedBefore:
                continue

            if (ClusterStates and
                    cluster['Status']['State'] not in ClusterStates):
                continue

            cluster_summaries.append(deepcopy(dict(
                (k, cluster[k])
                for k in ['Id', 'Name', 'Status', 'NormalizedInstanceHours'])))

        return dict(Clusters=cluster_summaries)

    def list_instances(self, ClusterId, InstanceGroupId=None,
                       InstanceGroupTypes=None, InstanceStates=None):
        """stripped-down simulation of list_instances() to support
        SSH tunneling; only includes state.status and the privateipaddress
        field.
        """
        if InstanceGroupId:
            raise NotImplementedError(
                "mock boto3 doesn't support instance IDs")

        if InstanceGroupTypes:
            _validate_param_type(InstanceGroupTypes, (list, tuple))
            for igt in InstanceGroupTypes:
                _validate_param_enum(igt, ['MASTER', 'CORE', 'TASK'])

        if InstanceStates:
            _validate_param_type(InstanceStates, (list, tuple))
            for ist in InstanceStates:
                _validate_param_enum(
                    ist,
                    ['AWAITING_FULFILLMENT', 'PROVISIONING', 'BOOTSTRAPPING',
                     'RUNNING', 'TERMINATED'])

        cluster = self._get_mock_cluster('ListInstances', ClusterId)

        instances = []  # to return

        for i, ig in enumerate(cluster['_InstanceGroups']):
            if InstanceGroupTypes and (
                    ig['InstanceGroupType'] not in InstanceGroupTypes):
                continue

            state = ig['Status']['State']

            if InstanceStates and state not in InstanceStates:
                continue

            for j in range(ig['RequestedInstanceCount']):
                # we construct mock instances from scratch
                instance = dict(
                    Status=dict(State=state),
                )

                if state not in ('PROVISIONING', 'AWAITING_FULLFILLMENT'):
                    # this is just an easy way to assign a unique IP
                    instance['PrivateIpAddress'] = '172.172.%d.%d' % (
                        i + 1, j + 1)

                instances.append(instance)

        return dict(Instances=instances)

    def list_instance_fleets(self, ClusterId):
        cluster = self._get_mock_cluster('ListInstanceFleets', ClusterId)

        if cluster.get('_InstanceGroups'):
            raise _InvalidRequestException(
                'ListInstanceFleets',
                'Instance groups and instance fleets are mutually exclusive.'
                ' The EMR cluster specified in the request uses instance'
                ' groups. The ListInstanceFleets operation does not support'
                ' clusters that use instance groups. Use the'
                ' ListInstanceGroups operation instead.')

        return dict(InstanceFleets=deepcopy(cluster['_InstanceFleets']))

    def list_instance_groups(self, ClusterId):
        cluster = self._get_mock_cluster('ListInstanceGroups', ClusterId)

        if cluster.get('_InstanceFleets'):
            raise _InvalidRequestException(
                'ListInstanceGroups',
                'Instance fleets and instance groups are mutually exclusive.'
                ' The EMR cluster specified in the request uses instance'
                ' fleets. The ListInstanceGroups operation does not support'
                ' clusters that use instance fleets. Use the'
                ' ListInstanceFleets operation instead.')

        return dict(InstanceGroups=deepcopy(cluster['_InstanceGroups']))

    def list_steps(self, ClusterId, StepIds=None, StepStates=None):
        return dict(Steps=self._list_steps(
            'ListSteps', ClusterId, StepIds=StepIds, StepStates=StepStates))

    def _list_steps(
            self, operation_name, ClusterId, StepIds=None, StepStates=None):
        """Helper for list_steps() and describe_step()."""
        cluster = self._get_mock_cluster(operation_name, ClusterId)

        # only last 1000 steps are visible
        mock_steps = cluster['_Steps'][-STEP_LIST_LIMIT:]

        # this is triggered even if StepIds and StepStates are []
        if not (StepIds is None or StepStates is None):
            raise _InvalidRequestException(
                operation_name,
                'Cannot specify both StepIds and StepStates.')

        results = []

        if StepIds:
            _validate_param_type(StepIds, (list, tuple))

            steps_by_id = dict((s['Id'], s) for s in mock_steps)

            for step_id in StepIds:
                if step_id not in steps_by_id:
                    raise _InvalidRequestException(
                        operation_name,
                        'Step id %r is not valid.' % step_id)

                # duplicate steps are allowed
                results.append(steps_by_id[step_id])
        else:
            for step in reversed(mock_steps):
                if StepStates and step['Status']['State'] not in StepStates:
                    continue

                results.append(step)

        return [deepcopy(step) for step in results]

    def terminate_job_flows(self, JobFlowIds):
        _validate_param_type(JobFlowIds, (list, tuple))

        if not JobFlowIds:
            raise _ValidationException(
                'TerminateJobFlows', 'The JobFlowId list cannot be empty')

        to_terminate = []
        cluster_ids_seen = set()

        for cluster_id in JobFlowIds:
            # the API doesn't allow duplicate IDs
            if cluster_id in cluster_ids_seen:
                raise _ValidationException(
                    'TerminateJobFlows',
                    'Specified job flow ID does not exist')
            cluster_ids_seen.add(cluster_id)

            to_terminate.append(
                self._get_mock_cluster('TerminateJobFlows', cluster_id))

        to_terminate.append(self.mock_emr_clusters[cluster_id])

        for cluster in to_terminate:
            # already terminated
            if cluster['Status']['State'] in (
                    'TERMINATED', 'TERMINATED_WITH_ERRORS'):
                continue

            # mark cluster as shutting down
            cluster['Status']['State'] = 'TERMINATING'
            cluster['Status']['StateChangeReason'] = dict(
                Code='USER_REQUEST',
                Message='Terminated by user request',
            )

            for step in cluster['_Steps']:
                if step['Status']['State'] == 'PENDING':
                    step['Status']['State'] = 'CANCELLED'
                elif step['Status']['State'] == 'RUNNING':
                    # pretty sure this is what INTERRUPTED is for
                    step['Status']['State'] = 'INTERRUPTED'

    def _get_step_output_uri(self, step_args):
        """Figure out the output dir for a step by parsing step.args
        and looking for an -output argument."""
        # parse in reverse order, in case there are multiple -output args
        for i, arg in reversed(list(enumerate(step_args[:-1]))):
            if arg == '-output':
                return step_args[i + 1]
        else:
            return None

    def _simulate_progress(self, cluster_id, now=None):
        """Simulate progress on the given cluster. This is automatically
        run when we call :py:meth:`describe_step`, and, when the cluster is
        ``TERMINATING``, :py:meth:`describe_cluster`.

        :type cluster_id: str
        :param cluster_id: fake cluster ID
        :type now: py:class:`datetime.datetime`
        :param now: alternate time to use as the current time (should be UTC)
        """
        # TODO: this doesn't actually update steps to CANCELLED when
        # cluster is shut down
        if now is None:
            now = _boto3_now()

        cluster = self.mock_emr_clusters[cluster_id]

        # allow clusters to get stuck
        if cluster.get('_DelayProgressSimulation', 0) > 0:
            cluster['_DelayProgressSimulation'] -= 1
            return

        # this code is pretty loose about updating StateChangeReason
        # (for the cluster, instance groups, and steps). Add this as needed.

        # if job is STARTING, move it along to BOOTSTRAPPING
        if cluster['Status']['State'] == 'STARTING':
            cluster['Status']['State'] = 'BOOTSTRAPPING'

            # master now has a hostname
            cluster['MasterPublicDnsName'] = 'master.%s.mock' % cluster['Id']

            # instances are now provisioned
            if cluster['InstanceCollectionType'] == 'INSTANCE_FLEET':
                for fleet in cluster['_InstanceFleets']:
                    fleet['ProvisionedOnDemandCapacity'] = fleet[
                        'TargetOnDemandCapacity']
                    fleet['ProvisionedSpotCapacity'] = fleet[
                        'TargetSpotCapacity']
                    fleet['Status']['State'] = 'BOOTSTRAPPING'

            else:
                for ig in cluster['_InstanceGroups']:
                    ig['RunningInstanceCount'] = ig['RequestedInstanceCount']
                    ig['Status']['State'] = 'BOOTSTRAPPING'

            return

        # if job is TERMINATING, move along to terminated
        if cluster['Status']['State'] == 'TERMINATING':
            code = cluster['Status']['StateChangeReason'].get('Code')
            if code and code.endswith('_FAILURE'):
                cluster['Status']['State'] = 'TERMINATED_WITH_ERRORS'
            else:
                cluster['Status']['State'] = 'TERMINATED'

            return

        # if job is done, nothing to do
        if cluster['Status']['State'] in ('TERMINATED',
                                          'TERMINATED_WITH_ERRORS'):
            return

        # if job is BOOTSTRAPPING, move it along to RUNNING and continue
        if cluster['Status']['State'] == 'BOOTSTRAPPING':
            cluster['Status']['State'] = 'RUNNING'

            if cluster['InstanceCollectionType'] == 'INSTANCE_FLEET':
                for fleet in cluster['_InstanceFleets']:
                    fleet['Status']['State'] = 'RUNNING'
            else:
                for ig in cluster['_InstanceGroups']:
                    ig['Status']['State'] = 'RUNNING'

        # at this point, should be RUNNING or WAITING
        assert cluster['Status']['State'] in ('RUNNING', 'WAITING')

        # simulate self-termination
        if cluster_id in self.mock_emr_self_termination:
            cluster['Status']['State'] = 'TERMINATING'
            cluster['Status']['StateChangeReason'] = dict(
                Code='INSTANCE_FAILURE',
                Message='The master node was terminated. ',  # sic
            )

            for step in cluster['_Steps']:
                if step['Status']['State'] in ('PENDING', 'RUNNING'):
                    step['Status']['State'] = 'CANCELLED'  # not INTERRUPTED

            return

        # try to find the next step, and advance it

        for step_num, step in enumerate(cluster['_Steps']):
            # skip steps that are already done
            if step['Status']['State'] in (
                    'COMPLETED', 'FAILED', 'CANCELLED', 'INTERRUPTED'):
                continue

            # found currently running step! handle it, then exit

            # start PENDING step
            if step['Status']['State'] == 'PENDING':
                step['Status']['State'] = 'RUNNING'
                step['Status']['Timeline']['StartDateTime'] = now
                return

            assert step['Status']['State'] == 'RUNNING'

            # check if we're supposed to have an error
            if (cluster_id, step_num) in self.mock_emr_failures:
                step['Status']['State'] = 'FAILED'

                if step['ActionOnFailure'] in (
                        'TERMINATE_CLUSTER', 'TERMINATE_JOB_FLOW'):

                    cluster['Status']['State'] = 'TERMINATING'
                    cluster['Status']['StateChangeReason']['Code'] = (
                        'STEP_FAILURE')
                    cluster['Status']['StateChangeReason']['Message'] = (
                        'Shut down as step failed')

                    for step in cluster['_Steps']:
                        if step['Status']['State'] in ('PENDING', 'RUNNING'):
                            step['Status']['State'] = 'CANCELLED'

                return

            # complete step
            step['Status']['State'] = 'COMPLETED'
            step['Status']['Timeline']['EndDateTime'] = now

            # create fake output if we're supposed to write to S3
            output_uri = self._get_step_output_uri(step['Config']['Args'])
            if output_uri and is_s3_uri(output_uri):
                mock_output = self.mock_emr_output.get(
                    (cluster_id, step_num)) or [b'']

                bucket_name, key_name = parse_s3_uri(output_uri)

                # write output to S3
                for i, part in enumerate(mock_output):
                    add_mock_s3_data(self.mock_s3_fs, {
                        bucket_name: {key_name + 'part-%05d' % i: part}})
            elif (cluster_id, step_num) in self.mock_emr_output:
                raise AssertionError(
                    "can't use output for cluster ID %s, step %d "
                    "(it doesn't output to S3)" %
                    (cluster_id, step_num))

            # done!
            # if this is the last step, continue to autotermination code, below
            if step_num < len(cluster['_Steps']) - 1:
                return

        # no pending steps. should we wait, or shut down?
        if cluster['AutoTerminate']:
            cluster['Status']['State'] = 'TERMINATING'
            cluster['Status']['StateChangeReason']['Code'] = (
                'ALL_STEPS_COMPLETED')
            cluster['Status']['StateChangeReason']['Message'] = (
                'Steps Completed')
        else:
            # just wait
            cluster['Status']['State'] = 'WAITING'
            cluster['Status']['StateChangeReason'] = {}

        return


# configuration munging

def _strip_hidden(d):
    """Return a (shallow) copy of the given dict, excluding fields starting
    with underscore."""
    return dict((k, v) for k, v in d.items() if not k.startswith('_'))


def _normalized_configurations(configurations):
    """The API will return an empty Properties list for configurations
    without properties set, and remove empty sub-configurations"""
    _validate_param_type(configurations, (list, tuple))
    configurations = list(configurations)

    for c in configurations:
        _validate_param_type(c, dict)
        c = dict(c)  # so we can modify it

        c.setdefault('properties', [])

        if 'configurations' in c:
            if c['configurations']:
                c['configurations'] = _normalized_configurations(
                    c['configurations'])
            else:
                del c['configurations']

    return configurations


# errors

def _validate_param(params, name, type=None):
    """Check that the param *name* is found in *params*, and if
    *type* is set, validate that it has the proper type.

    *type* may also be a tuple (multiple types) or a list
    (multiple values to match)
    """
    if name not in params:
        raise ParamValidationError(
            report='Missing required parameter in input: "%s"' % name)

    if type:
        if isinstance(type, list):
            _validate_param_enum(params[name], type)
        else:
            _validate_param_type(params[name], type)


def _validate_param_type(value, type):
    """Raise ParamValidationError if *value* isn't an instance of
    *type*."""
    if not isinstance(value, type):
        raise ParamValidationError(
            report=('%r is not an instance of %r' % (value, type)))


def _validate_param_enum(value, allowed):
    if value not in allowed:
        # the actual error is more verbose than this
        raise ParamValidationError(
            report=('Value %r failed to satisfy constraint: Member must'
                    ' satisfy value enum set: [%s]' % ', '.join(allowed)))


def _InvalidRequestException(operation_name, message):
    # boto3 reports this as a botocore.exceptions.InvalidRequestException,
    # but that's not something you can actually import
    return AttributeError(
        'An error occurred (InvalidRequestException) when calling the'
        ' %s operation: %s' % (operation_name, message))


def _ValidationException(operation_name, message):
    return ClientError(
        dict(
            Error=dict(
                Code='ValidationException',
                Message=message,
            ),
            ResponseMetadata=dict(
                HTTPStatusCode=404,
            ),
        ),
        operation_name,
    )
