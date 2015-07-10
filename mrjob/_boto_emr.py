# Copyright (c) 2010 Spotify AB
# Copyright (c) 2010 Jeremy Thurgood <firxen+boto@gmail.com>
# Copyright (c) 2010-2011 Yelp
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
"""Code from boto 2.35.0 to support "cluster" EMR API calls"""
import boto.utils
from boto.emr.emrobject import EmrObject
from boto.resultset import ResultSet


# from boto/emr/connection.py (these are EMRConnection methods in boto)

def describe_cluster(emr_conn, cluster_id):
    """
    Describes an Elastic MapReduce cluster

    :type cluster_id: str
    :param cluster_id: The cluster id of interest
    """
    params = {
        'ClusterId': cluster_id
    }
    return emr_conn.get_object('DescribeCluster', params, Cluster)


def list_bootstrap_actions(self, cluster_id, marker=None):
    """
    Get a list of bootstrap actions for an Elastic MapReduce cluster

    :type cluster_id: str
    :param cluster_id: The cluster id of interest
    :type marker: str
    :param marker: Pagination marker
    """
    params = {
        'ClusterId': cluster_id
    }

    if marker:
        params['Marker'] = marker

    return self.get_object('ListBootstrapActions', params, BootstrapActionList)


def list_clusters(emr_conn, created_after=None, created_before=None,
                  cluster_states=None, marker=None):
    """
    List Elastic MapReduce clusters with optional filtering

    :type created_after: datetime
    :param created_after: Bound on cluster creation time
    :type created_before: datetime
    :param created_before: Bound on cluster creation time
    :type cluster_states: list
    :param cluster_states: Bound on cluster states
    :type marker: str
    :param marker: Pagination marker
    """
    params = {}
    if created_after:
        params['CreatedAfter'] = created_after.strftime(
            boto.utils.ISO8601)
    if created_before:
        params['CreatedBefore'] = created_before.strftime(
            boto.utils.ISO8601)
    if marker:
        params['Marker'] = marker

    if cluster_states:
        emr_conn.build_list_params(params, cluster_states, 'ClusterStates.member')

    return emr_conn.get_object('ListClusters', params, ClusterSummaryList)


def list_steps(self, cluster_id, step_states=None, marker=None):
    """
    List cluster steps

    :type cluster_id: str
    :param cluster_id: The cluster id of interest
    :type step_states: list
    :param step_states: Filter by step states
    :type marker: str
    :param marker: Pagination marker
    """
    params = {
        'ClusterId': cluster_id
    }

    if marker:
        params['Marker'] = marker

    if step_states:
        self.build_list_params(params, step_states, 'StepStateList.member')

    return self.get_object('ListSteps', params, StepSummaryList)


# from boto/emr/emrobject.py

class Application(EmrObject):
    Fields = set([
        'Name',
        'Version',
        'Args',
        'AdditionalInfo'
    ])


class Arg(EmrObject):
    def __init__(self, connection=None):
        self.value = None

    def endElement(self, name, value, connection):
        self.value = value


class BootstrapAction(EmrObject):
    Fields = set([
        'Args',
        'Name',
        'Path',
        'ScriptPath',
    ])

    def startElement(self, name, attrs, connection):
        if name == 'Args':
            self.args = ResultSet([('member', Arg)])
            return self.args


class BootstrapActionList(EmrObject):
    Fields = set([
        'Marker'
    ])

    def __init__(self, connection=None):
        self.connection = connection
        self.actions = None

    def startElement(self, name, attrs, connection):
        if name == 'BootstrapActions':
            self.actions = ResultSet([('member', BootstrapAction)])
            return self.actions
        else:
            return None


class Cluster(EmrObject):
    Fields = set([
        'Id',
        'Name',
        'LogUri',
        'RequestedAmiVersion',
        'RunningAmiVersion',
        'AutoTerminate',
        'TerminationProtected',
        'VisibleToAllUsers',
        'MasterPublicDnsName',
        'NormalizedInstanceHours',
        'ServiceRole'
    ])

    def __init__(self, connection=None):
        self.connection = connection
        self.status = None
        self.ec2instanceattributes = None
        self.applications = None
        self.tags = None

    def startElement(self, name, attrs, connection):
        if name == 'Status':
            self.status = ClusterStatus()
            return self.status
        elif name == 'Ec2InstanceAttributes':
            self.ec2instanceattributes = Ec2InstanceAttributes()
            return self.ec2instanceattributes
        elif name == 'Applications':
            self.applications = ResultSet([('member', Application)])
            return self.applications
        elif name == 'Tags':
            self.tags = ResultSet([('member', KeyValue)])
            return self.tags
        else:
            return None


class ClusterStateChangeReason(EmrObject):
    Fields = set([
        'Code',
        'Message'
    ])


class ClusterStatus(EmrObject):
    Fields = set([
        'State',
        'StateChangeReason',
        'Timeline'
    ])

    def __init__(self, connection=None):
        self.connection = connection
        self.timeline = None

    def startElement(self, name, attrs, connection):
        if name == 'Timeline':
            self.timeline = ClusterTimeline()
            return self.timeline
        elif name == 'StateChangeReason':
            self.statechangereason = ClusterStateChangeReason()
            return self.statechangereason
        else:
            return None


class ClusterSummary(EmrObject):
    Fields = set([
        'Id',
        'Name',
        'NormalizedInstanceHours'
    ])

    def __init__(self, connection):
        self.connection = connection
        self.status = None

    def startElement(self, name, attrs, connection):
        if name == 'Status':
            self.status = ClusterStatus()
            return self.status
        else:
            return None


class ClusterSummaryList(EmrObject):
    Fields = set([
        'Marker'
    ])

    def __init__(self, connection):
        self.connection = connection
        self.clusters = None

    def startElement(self, name, attrs, connection):
        if name == 'Clusters':
            self.clusters = ResultSet([('member', ClusterSummary)])
            return self.clusters
        else:
            return None


class ClusterTimeline(EmrObject):
    Fields = set([
        'CreationDateTime',
        'ReadyDateTime',
        'EndDateTime',
        # !!! steps have StartDateTime, not ReadyDateTime
        # !!! The underlying problem is that there is no separate StepsTimeline
        # !!! class; see https://github.com/boto/boto/issues/3268
        'StartDateTime',
    ])


class Ec2InstanceAttributes(EmrObject):
    Fields = set([
        'Ec2KeyName',
        'Ec2SubnetId',
        'Ec2AvailabilityZone',
        'IamInstanceProfile'
    ])


class KeyValue(EmrObject):
    Fields = set([
        'Key',
        'Value',
    ])


class StepConfig(EmrObject):
    Fields = set([
        'Jar',
        'MainClass'
    ])

    def __init__(self, connection=None):
        self.connection = connection
        self.properties = None
        self.args = None

    def startElement(self, name, attrs, connection):
        if name == 'Properties':
            self.properties = ResultSet([('member', KeyValue)])
            return self.properties
        elif name == 'Args':
            self.args = ResultSet([('member', Arg)])
            return self.args
        else:
            return None


class StepSummary(EmrObject):
    Fields = set([
        'Id',
        'Name'
    ])

    def __init__(self, connection=None):
        self.connection = connection
        self.status = None
        self.config = None

    def startElement(self, name, attrs, connection):
        if name == 'Status':
            self.status = ClusterStatus()
            return self.status
        elif name == 'Config':
            self.config = StepConfig()
            return self.config
        else:
            return None


class StepSummaryList(EmrObject):
    Fields = set([
        'Marker'
    ])

    def __init__(self, connection=None):
        self.connection = connection
        self.steps = None

    def startElement(self, name, attrs, connection):
        if name == 'Steps':
            self.steps = ResultSet([('member', StepSummary)])
            return self.steps
        else:
            return None
