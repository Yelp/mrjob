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

from botocore.exceptions import ClientError
from botocore.exceptions import ParamValidationError

from mrjob.aws import _DEFAULT_AWS_REGION
from mrjob.aws import _boto3_now
from mrjob.compat import map_version
from mrjob.compat import version_gte
from mrjob.conf import combine_values
from mrjob.parse import is_s3_uri
from mrjob.parse import parse_s3_uri
from mrjob.py2 import integer_types
from mrjob.py2 import string_types

from tests.py2 import unittest

from .s3 import add_mock_s3_data
from .util import MockObject
from .util import MockPaginator


class Boto2TestSkipper(object):
    def __call__(self, *args, **kwargs):
        raise unittest.SkipTest('old boto 2 test')

    def __getattr__(self, name):
        self()

MockEmrConnection = Boto2TestSkipper()
MockEmrObject = Boto2TestSkipper()
err_xml = Boto2TestSkipper()
to_iso8601 = Boto2TestSkipper()
_decode_configurations_from_api = Boto2TestSkipper()
_list_all_steps = Boto2TestSkipper()
_yield_all_instance_groups = Boto2TestSkipper()
_yield_all_clusters = Boto2TestSkipper()


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

STEP_LIMIT = 256


# list_clusters() only returns this many results at a time
DEFAULT_MAX_CLUSTERS_RETURNED = 50

# list_steps() only returns this many results at a time
DEFAULT_MAX_STEPS_RETURNED = 50

# need to fill in a version for non-Hadoop applications
DUMMY_APPLICATION_VERSION = '0.0.0'


class MockEMRClient(object):
    """Mock out boto3 EMR clients. This actually handles a small
    state machine that simulates EMR clusters."""

    DEFAULT_MAX_ITEMS = 50

    OPERATION_NAME_TO_RESULT_KEY = dict(
        list_bootstrap_actions='BootstrapActions',
        list_clusters='Clusters',
        list_instance_groups='InstanceGroups',
        list_instances='Instances',
        list_steps='Steps',
    )

    _enforce_strict_ssl = Boto2TestSkipper()

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
            # not entirely sure why boto3 1.4.4 uses a different format for
            # us-east-1, but there it is. according to AWS docs, the canonical
            # host name is elasticmapreduce.<region>.amazonaws.com.
            if endpoint_url == _DEFAULT_AWS_REGION:
                endpoint_url = (
                    'https://elasticmapreduce.us-east-1.amazonaws.com')
            else:
                endpoint_url = (
                    'https://%s.elasticmapreduce.amazonaws.com' % region_name)

        self.meta = MockObject(
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
        # _InstanceGroups: as described by list_instance_groups()
        # _Steps: as decribed by list_steps(), but not reversed
        #
        # TODO: at some point when we implement instance fleets,
        # _InstanceGroups will become optional
        #
        # TODO: update simulate_progress() to add MasterPublicDnsName
        cluster = dict(
            _BootstrapActions=[],
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
            Id='',
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

        # Id
        cluster['Id'] = (
            kwargs.pop('_Id', None) or
            'j-MOCKCLUSTER%d' % len (self.mock_emr_clusters))

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
            _validate_param(Instances, 'Ec2SubnetId', string_types)
            cluster['Ec2InstanceAttributes']['Ec2SubnetId'] = (
                Instances.pop('Ec2SubnetId'))

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
            cluster['Ec2AvailabilityZone'] = Placement['AvailabilityZone']

        if 'InstanceGroups' in Instances:
            if any(x in Instances for x in ('MasterInstanceType',
                                            'SlaveInstanceType',
                                            'InstanceCount')):
                raise _error(
                    'Please configure instances using one and only one of the'
                    ' following: instance groups; instance fleets; instance'
                    ' count, master and slave instance type.')

            self._add_instance_groups(
                operation_name, Instances.pop('InstanceGroups'), cluster)
        # TODO: will need to support instance fleets at some point
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
        if cluster.get('_InstanceGroups'):
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
                    " set: [SPOT, ON_DEMAND]" % (role, i + 1))
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
            if len(cluster['_Steps']) + len(Steps) > STEP_LIMIT:
                raise _ValidationException(
                    operation_name,
                    'Maximum number of steps for job flow exceeded')
        else:
            # otherwise, only active and pending steps count
            num_active_steps = sum(
                1 for step in cluster['_Steps']
                if step['Status']['State'] in (
                        'PENDING', 'PENDING_CANCELLED', 'RUNNING'))
            if num_active_steps + len(Steps) > STEP_LIMIT:
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
                Id='s-MOCKSTEP%d' % (len(cluster['_Steps']) + 1),
                Name='',
                Status=dict(
                    State='PENDING',
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
                    "mock_boto3 doesn't support these HadoopJarStep params: %s" %
                    ', '.join(sorted(HadoopJarStep)))

            if Step:
                raise NotImplementedError(
                    "mock_boto3 doesn't support these step params: %s" %
                    ', '.join(sorted(Step)))

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

        tags_dict = dict((t['Key'], t['Value']) for t in cluster['Tags'])
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
            self.simulate_progress(ClusterId)

        # hide _ fields
        cluster = dict(
            (k, deepcopy(v)) for k, v in cluster.items()
            if not k.startswith('_'))

        return dict(Cluster=cluster)

    def describe_step(self, **kwargs):
        self._enforce_strict_ssl()

        # simulate progress, to support _wait_for_steps_to_complete()
        self.simulate_progress(cluster_id)

        # make sure that we only call list_steps() when we've patched
        # around https://github.com/boto/boto/issues/3268
        if 'StartDateTime' not in boto.emr.emrobject.ClusterTimeline.Fields:
            raise Exception('called un-patched version of describe_step()!')

        cluster = self._get_mock_cluster(cluster_id)

        for step in cluster._steps:
            if step.id == step_id:
                return step

        raise boto.exception.EmrResponseError(
            400, 'Bad Request', body=err_xml(
                "Step id '%s' is not valid." % step_id))

    def list_bootstrap_actions(self, **kwargs):
        self._enforce_strict_ssl()

        if marker is not None:
            raise NotImplementedError(
                'marker not simulated for ListBootstrapActions')

        cluster = self._get_mock_cluster(cluster_id)

        return MockEmrObject(actions=cluster._bootstrapactions)

    def list_clusters(self, **kwargs):
        self._enforce_strict_ssl()

        # summaries of cluster state, to return
        cluster_summaries = []

        # add markers to clusters
        marked_clusters = [
            ((cluster.status.timeline.creationdatetime, cluster.id), cluster)
            for cluster in self.mock_emr_clusters.values()]
        marked_clusters.sort(reverse=True)

        # *marker* is just the marker for the last cluster we returned

        for cluster_marker, cluster in marked_clusters:
            if marker is not None and cluster_marker <= marker:
                continue

            # stop if we hit pagination limit
            if len(cluster_summaries) >= self.max_clusters_returned:
                break

            created = cluster.status.timeline.creationdatetime

            if created_after is not None and created < created_after:
                continue

            if created_before is not None and created > created_before:
                continue

            state = cluster.status.state

            if not (cluster_states is None or state in cluster_states):
                continue

            cluster_summaries.append(MockEmrObject(
                id=cluster.id,
                name=cluster.name,
                normalizedinstancehours=cluster.normalizedinstancehours,
                status=cluster.status))
        else:
            # we went through all clusters, no need to call again
            cluster_marker = None

        return MockEmrObject(clusters=cluster_summaries, marker=cluster_marker)

    def list_instances(self, **kwargs):
        """stripped-down simulation of list_instances() to support
        SSH tunneling; only includes state.status and the privateipaddress
        field.
        """
        self._enforce_strict_ssl()

        if instance_group_id is not None:
            raise NotImplementedError(
                'instance_group_id not simulated for ListInstances')

        if marker is not None:
            raise NotImplementedError(
                'marker not simulated for ListInstances')

        cluster = self._get_mock_cluster(cluster_id)

        instances = []

        for i, ig in enumerate(cluster._instancegroups):
            if instance_group_types and (
                    ig.instancegrouptype not in instance_group_types):
                continue

            for j in range(int(ig.requestedinstancecount)):
                instance = MockEmrObject()

                state = ig.status.state

                instance.status = MockEmrObject(state=state)

                if state not in ('PROVISIONING', 'AWAITING_FULLFILLMENT'):
                    # this is just an easy way to assign a unique IP
                    instance.privateipaddress = '172.172.%d.%d' % (
                        i + 1, j + 1)

                instances.append(instance)

        return MockEmrObject(instances=instances)

    def list_instance_groups(self, **kwargs):
        self._enforce_strict_ssl()

        # TODO: not sure what order API returns instance groups in,
        # but doesn't matter for us, as our code treats them like
        # a dictionary. See #1316.

        if marker is not None:
            raise NotImplementedError(
                'marker not simulated for ListInstanceGroups')

        cluster = self._get_mock_cluster(cluster_id)

        return MockEmrObject(instancegroups=cluster._instancegroups)

    def list_steps(self, **kwargs):
        self._enforce_strict_ssl()

        # make sure that we only call list_steps() when we've patched
        # around https://github.com/boto/boto/issues/3268
        if 'StartDateTime' not in boto.emr.emrobject.ClusterTimeline.Fields:
            raise Exception('called un-patched version of list_steps()!')

        steps = self._get_mock_cluster(cluster_id)._steps

        steps_listed = []

        # *marker* was the index of the last step we listed
        for index in reversed(range(marker or len(steps))):
            step = steps[index]
            if step_states is None or step.status.state in step_states:
                steps_listed.append(step)

            if len(steps_listed) >= self.max_steps_returned:
                break
        else:
            index = None  # listed all steps, no need to call again

        return MockEmrObject(steps=steps_listed, marker=index)

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
            if arg.value == '-output':
                return step_args[i + 1].value
        else:
            return None

    def simulate_progress(self, cluster_id, now=None):
        """Simulate progress on the given cluster. This is automatically
        run when we call :py:meth:`describe_step`, and, when the cluster is
        ``TERMINATING``, :py:meth:`describe_cluster`.

        :type cluster_id: str
        :param cluster_id: fake cluster ID
        :type now: py:class:`datetime.datetime`
        :param now: alternate time to use as the current time (should be UTC)
        """
        self._enforce_strict_ssl()

        # TODO: this doesn't actually update steps to CANCELLED when
        # cluster is shut down
        if now is None:
            now = _boto3_now()

        cluster = self._get_mock_cluster(cluster_id)

        # allow clusters to get stuck
        if getattr(cluster, 'delay_progress_simulation', 0) > 0:
            cluster.delay_progress_simulation -= 1
            return

        # this code is pretty loose about updating statechangereason
        # (for the cluster, instance groups, and steps). Add this as needed.

        # if job is STARTING, move it along to BOOTSTRAPPING
        if cluster.status.state == 'STARTING':
            cluster.status.state = 'BOOTSTRAPPING'
            # instances are now provisioned
            for ig in cluster._instancegroups:
                ig.runninginstancecount = ig.requestedinstancecount,
                ig.status.state = 'BOOTSTRAPPING'

            return

        # if job is TERMINATING, move along to terminated
        if cluster.status.state == 'TERMINATING':
            code = getattr(getattr(cluster.status, 'statechangereason', None),
                           'code', None)

            if code and code.endswith('_FAILURE'):
                cluster.status.state = 'TERMINATED_WITH_ERRORS'
            else:
                cluster.status.state = 'TERMINATED'

            return

        # if job is done, nothing to do
        if cluster.status.state in ('TERMINATED', 'TERMINATED_WITH_ERRORS'):
            return

        # if job is BOOTSTRAPPING, move it along to RUNNING and continue
        if cluster.status.state == 'BOOTSTRAPPING':
            cluster.status.state = 'RUNNING'
            for ig in cluster._instancegroups:
                ig.status.state = 'RUNNING'

        # at this point, should be RUNNING or WAITING
        assert cluster.status.state in ('RUNNING', 'WAITING')

        # simulate self-termination
        if cluster_id in self.mock_emr_self_termination:
            cluster.status.state = 'TERMINATING'
            cluster.status.statechangereason = MockEmrObject(
                code='INSTANCE_FAILURE',
                message='The master node was terminated. ',  # sic
            )

            for step in cluster._steps:
                if step.status.state in ('PENDING', 'RUNNING'):
                    step.status.state = 'CANCELLED'  # not INTERRUPTED

            return

        # try to find the next step, and advance it

        for step_num, step in enumerate(cluster._steps):
            # skip steps that are already done
            if step.status.state in (
                    'COMPLETED', 'FAILED', 'CANCELLED', 'INTERRUPTED'):
                continue

            # found currently running step! handle it, then exit

            # start PENDING step
            if step.status.state == 'PENDING':
                step.status.state = 'RUNNING'
                step.status.timeline.startdatetime = to_iso8601(now)
                return

            assert step.status.state == 'RUNNING'

            # check if we're supposed to have an error
            if (cluster_id, step_num) in self.mock_emr_failures:
                step.status.state = 'FAILED'

                if step.actiononfailure in (
                        'TERMINATE_CLUSTER', 'TERMINATE_JOB_FLOW'):

                    cluster.status.state = 'TERMINATING'
                    cluster.status.statechangereason.code = 'STEP_FAILURE'
                    cluster.status.statechangereason.message = (
                        'Shut down as step failed')

                return

            # complete step
            step.status.state = 'COMPLETED'
            step.status.timeline.enddatetime = to_iso8601(now)

            # create fake output if we're supposed to write to S3
            output_uri = self._get_step_output_uri(step.config.args)
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
            if step_num < len(cluster._steps) - 1:
                return

        # no pending steps. should we wait, or shut down?
        if cluster.autoterminate == 'true':
            cluster.status.state = 'TERMINATING'
            cluster.status.statechangereason.code = 'ALL_STEPS_COMPLETED'
            cluster.status.statechangereason.message = (
                'Steps Completed')
        else:
            # just wait
            cluster.status.state = 'WAITING'
            cluster.status.statechangereason = MockEmrObject()

        return


# configuration munging

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
