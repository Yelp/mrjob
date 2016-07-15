# Copyright 2015-2016 Yelp
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
"""Patches to classes in boto.emr.emrobject to correct bugs and support
4.x AMIs.

Don't depend on code in this module; it might go away in later 0.5.x versions
of mrjob!
"""
import boto.emr.connection
from boto.emr.emrobject import Cluster
from boto.emr.emrobject import ClusterTimeline
from boto.emr.emrobject import EmrObject
from boto.resultset import ResultSet


def _patched_describe_cluster(emr_conn, *args, **kwargs):
    """Wrapper for :py:meth:`boto.emr.EmrConnection.list_steps()`
    that adds the ReleaseLabel and Configurations fields.
    """
    # monkey-patch boto.emr.connection, because that's what
    # describe_cluster() references. Not using patch here because it's
    # an external dependency in Python 2
    try:
        boto.emr.connection.Cluster = _PatchedCluster
        return emr_conn.describe_cluster(*args, **kwargs)
    finally:
        boto.emr.connection.Cluster = Cluster


def _patched_list_steps(emr_conn, *args, **kwargs):
    """Wrapper for :py:meth:`boto.emr.EmrConnection.list_steps()`
    that works around around `boto's startdatetime bug
    <https://github.com/boto/boto/issues/3268>`__.
    """
    # technically, steps shouldn't be using the ClusterTimeline class at
    # all; StepSummary should use a StepStatus class instead of ClusterStatus,
    # and StepStatus should have its own StepTimeline class. But that doesn't
    # make a difference for mrjob.

    # monkey-patch boto.emr.emrobject, because that's what
    # StepSummaryList references. Not using patch here because it's
    # an external dependency in Python 2
    try:
        boto.emr.emrobject.ClusterTimeline = _PatchedClusterTimeline
        return emr_conn.list_steps(*args, **kwargs)
    finally:
        boto.emr.emrobject.ClusterTimeline = ClusterTimeline


def _patched_describe_step(emr_conn, *args, **kwargs):
    """Wrapper for :py:meth:`boto.emr.EmrConnection.list_steps()`
    that works around around `boto's startdatetime bug
    <https://github.com/boto/boto/issues/3268>`__."""
    # see comment in _patched_list_steps() for details
    try:
        boto.emr.emrobject.ClusterTimeline = _PatchedClusterTimeline
        return emr_conn.describe_step(*args, **kwargs)
    finally:
        boto.emr.emrobject.ClusterTimeline = ClusterTimeline


class _Configuration(EmrObject):
    """The Configuration class, per
    http://docs.aws.amazon.com/ElasticMapReduce/latest/API/API_Cluster.html.

    Looks like in practice, Applications is returned with 4.x AMIs too.
    """
    Fields = set(['Classification'])

    def __init__(self, connection=None):
        super(_Configuration, self).__init__(connection=connection)
        self.configurations = None
        self.properties = None

    def startElement(self, name, attrs, connection):
        if name == 'Configurations':
            # configurations can contain themselves
            self.configurations = ResultSet([('member', _Configuration)])
            return self.configurations
        elif name == 'Properties':
            self.properties = ResultSet([('entry', _KeyValueEntry)])
            return self.properties


class _KeyValueEntry(EmrObject):
    """Like KeyValue, but used for Properties in _Configurations."""
    Fields = set([
        'key',
        'value',
    ])


class _PatchedCluster(Cluster):
    """Cluster class, plus the ReleaseLabel and Configurations fields."""

    # add ReleaseLabel parameter
    Fields = Cluster.Fields | set(['ReleaseLabel'])

    # add configurations list
    def __init__(self, connection=None):
        super(_PatchedCluster, self).__init__(connection=connection)
        self.configurations = None

    def startElement(self, name, attrs, connection):
        if name == 'Configurations':
            self.configurations = ResultSet([('member', _Configuration)])
            return self.configurations

        return super(_PatchedCluster, self).startElement(
            name, attrs, connection)


class _PatchedClusterTimeline(ClusterTimeline):
    """ClusterTimeline, plus the StartDateTime field."""

    Fields = ClusterTimeline.Fields | set(['StartDateTime'])
