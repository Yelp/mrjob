# Copyright 2018 Yelp
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
"""Submit a spark job using mrjob runners."""

_USAGE = ('%(prog)s spark-submit [-r <runner>] [options]'
          ' <python file | app jar> [app arguments]')


# opts, mrjob or otherwise, used by the spark-submit utility
_SPARK_SUBMIT_OPTS = [
    ('spark_master', dict(
        help=('Hadoop runner only: spark://host:port, mesos://host:port, yarn,'
              ' k8s://https://host:port, or local. Defaults to yarn')
        switch_aliases={'--spark-master': '--master'}
    )),



]



_CORE_OPTS = {


}


# map from a mrjob switch to the Spark equivalent
_SPARK_SUBMIT_SWITCHES = {
    '--jobconf': '--conf',
    '--libjars': '--jars',
    '--spark-deploy-mode': '--deploy-mode',
    '--spark-master': '--master',
}
