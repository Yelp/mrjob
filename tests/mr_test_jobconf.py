# Copyright 2009-2012 Yelp
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
"""Tests for JobConf Environment Variables
"""
from mrjob.compat import jobconf_from_env
from mrjob.job import MRJob

JOBCONF_LIST = [
    'mapreduce.job.id',
    'mapreduce.job.local.dir',
    'mapreduce.task.id',
    'mapreduce.task.attempt.id',
    'mapreduce.task.ismap',
    'mapreduce.task.partition',
    'mapreduce.map.input.file',
    'mapreduce.map.input.start',
    'mapreduce.map.input.length',
    'mapreduce.task.output.dir',
    'mapreduce.job.cache.archives',
    'mapreduce.job.cache.files',
    'mapreduce.job.cache.local.archives',
    'mapreduce.job.cache.local.files',
    'user.defined'
]


class MRTestJobConf(MRJob):

    def mapper_init(self):
        for jobconf in JOBCONF_LIST:
            yield (jobconf, jobconf_from_env(jobconf))


if __name__ == '__main__':
    MRTestJobConf.run()
