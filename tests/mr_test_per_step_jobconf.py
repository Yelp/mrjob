# Copyright 2013 Lyft and Contributors
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
from mrjob.compat import jobconf_from_env
from mrjob.job import MRJob
from mrjob.step import MRStep

JOBCONF_LIST = [
    'mapred.map.tasks',
    'mapreduce.job.local.dir',
    'user.defined',
]


class MRTestPerStepJobConf(MRJob):

    def mapper_init(self):
        self.increment_counter('count', 'mapper_init', 1)
        for jobconf in JOBCONF_LIST:
            yield ((self.options.step_num, jobconf),
                   jobconf_from_env(jobconf, None))

    def mapper(self, key, value):
        yield key, value

    def steps(self):
        return([
            MRStep(mapper_init=self.mapper_init),
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   jobconf={'user.defined': 'nothing',
                            'mapred.map.tasks': 4})])


if __name__ == '__main__':
    MRTestPerStepJobConf.run()
