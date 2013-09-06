# Copyright 2013 Lyft
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
"""Tests for setting JobConf Environment Variables on a per-step basis
"""
from mrjob.compat import get_jobconf_value
from mrjob.job import MRJob

JOBCONF_LIST = [
    'user.defined'
]


class MRTestPerStepJobConf(MRJob):

    def mapper_init(self):
        for jobconf in JOBCONF_LIST:
            yield (jobconf, get_jobconf_value(jobconf))

    def mapper2(self, jobconf, jobconf_value):
        yield jobconf, [jobconf_value, get_jobconf_value(jobconf)]

    def steps(self):
        return([
            self.mr(mapper_init=self.mapper_init),
            self.mr(mapper=self.mapper2,
                    jobconf={'user.defined': 'nothing'})])


if __name__ == '__main__':
    MRTestPerStepJobConf.run()
