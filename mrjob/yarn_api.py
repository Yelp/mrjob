# -*- coding: utf-8 -*-
# Copyright 2009-2018 Yelp and Contributors
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
"""Provides basic access to the YARN API.

The only API required by mrjob, and thus implemented, is the ResourceManager's
REST API. More documentation on this API can be found here:
    https://hadoop.apache.org/docs/current/hadoop-yarn/
            hadoop-yarn-site/ResourceManagerRest.html
"""
import requests


class BaseAPI(object):
    """Simplistic class supporting GET, PUT, and POST API calls."""

    def __init__(self, host, port, timeout):
        self.host = host
        self.port = port
        self.timeout = timeout

    def _check_for_success(self, request_type, response):
        if response.status_code != requests.codes.ok:
            raise Exception('{} request received {}: {}'.format(
                            request_type,
                            response.status_code,
                            response.text))
        return response.json()

    def _get(self, api, params={}):
        endpoint = 'http://{}:{}{}'.format(self.host, self.port, api)
        response = requests.get(endpoint, params=params, timeout=self.timeout)
        return self._check_for_success('GET', response)

    def _put(self, api, data={}):
        endpoint = 'http://{}:{}{}'.format(self.host, self.port, api)
        response = requests.put(endpoint, data=data, timeout=self.timeout)
        return self._check_for_success('PUT', response)

    def _post(self, api, data={}):
        endpoint = 'http://{}:{}{}'.format(self.host, self.port, api)
        response = requests.post(endpoint, data=data, timeout=self.timeout)
        return self._check_for_success('POST', response)


class YarnResourceManager(BaseAPI):
    """Wraps API calls to the YARN ResourceManager API. Provides a subset of
    API calls as only limited functionality is needed.
    """

    RESOURCE_MANAGER_PORT = 8088

    def __init__(self, host, timeout):
        super(YarnResourceManager, self).__init__(
            host,
            self.RESOURCE_MANAGER_PORT,
            timeout
        )

    ### General cluster calls

    def get_cluster_info(self):
        """Returns overall information about the cluster.

        Has no query parameters.
        """
        return self._get('/ws/v1/cluster/info')['clusterInfo']

    def get_cluster_metrics(self):
        """Returns overall metrics about the cluster.

        Has no query parameters.
        """
        return self._get('/ws/v1/cluster/metrics')['clusterMetrics']

    def get_cluster_scheduler_info(self):
        """Returns information about the current scheduler configured.

        Has no query parameters.
        """
        return self._get('/ws/v1/cluster/scheduler')['scheduler']

    ### Node calls

    def list_nodes(self, states=[]):
        """Returns a list of information pertaining to each node.

        Has a single query parameter, state. Valid options are
            DECOMMISSIONED  - Node is out of service.
            DECOMMISSIONING - Node decommission is in progress.
            LOST            - Node has not sent a heartbeat for some time.
            NEW             - New node.
            REBOOTED        - Node has rebooted.
            RUNNING         - Running node.
            SHUTDOWN        - Node has shutdown gracefully.
            UNHEALTHY       - Node is unhealthy.
        """
        endpoint = '/ws/v1/cluster/nodes'
        return self._get(endpoint, {'states': states})['nodes']['node']

    def get_node_info(self, nodeid):
        """Returns information on the specified node objects.

        Has no query parameters.
        """
        endpoint = '/ws/v1/cluster/nodes/{nodeid}'.format(nodeid=nodeid)
        return self._get(endpoint)['node']

    ### Application calls

    def list_applications(self, states=[]):
        """Returns a list of information pertaining to each application.

        Has query parameter, state. Valid options are
            ACCEPTED   - Application has been accepted by the scheduler.
            FAILED     - Application which failed.
            FINISHED   - Application which finished successfully.
            KILLED     - Application which was terminated by a user or admin.
            NEW        - Application which was just created.
            NEW_SAVING - Application which is being saved.
            RUNNING    - Application which is currently running.
            SUBMITTED  - Application which has been submitted.

        There are other query parameters available, but this class does not
        currently support them.
        """
        endpoint = '/ws/v1/cluster/apps'
        return self._get(endpoint, {'states': states})['apps']['app']

    def new_application(self):
        """Creates a new application. Returns information about this
        application.

        Has no query parameters.
        """
        return self._post('/ws/v1/cluster/apps/new-application')

    def get_application_info(self, appid):
        """Returns information on the specified application.

        Has no query parameters.
        """
        endpoint = '/ws/v1/cluster/apps/{appid}'.format(appid=appid)
        return self._get(endpoint)['app']

    def get_app_statistics(self, states=[], applicationTypes=[]):
        """Returns statistics on the specified application.

        Has no query parameters state and applicationType. See
        :py:meth:`list_applications` for supported states.
        """
        params = {'state': states, 'applicationTypes': applicationTypes}
        return self._get('/ws/v1/cluster/appstatistics', params)['appStatInfo']

    def get_application_attempts(self, appid):
        """Returns a list of application attempts from the specified
        application.

        Has no query parameters.
        """
        endpoint = '/ws/v1/cluster/apps/{appid}/appattempts' \
                   .format(appid=appid)
        return self._get(endpoint)['appAttempts']

    def get_application_state(self, appid):
        """Returns the state of the specified application. See
        :py:meth:`list_applications` for possible states.

        Has no query parameters.
        """
        endpoint = '/ws/v1/cluster/apps/{appid}/state'.format(appid=appid)
        return self._get(endpoint)['state']

    def get_application_queue(self, appid):
        """Returns the query on the specified application is in.

        Has no query parameters.
        """
        endpoint = '/ws/v1/cluster/apps/{appid}/queue'.format(appid=appid)
        return self._get(endpoint)['queue']

    def get_application_priority(self, appid):
        """Returns the priority of the specified application.

        Has no query parameters.
        """
        endpoint = '/ws/v1/cluster/apps/{appid}/priority'.format(appid=appid)
        return self._get(endpoint)['priority']
