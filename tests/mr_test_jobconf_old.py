# Copyright 2009-2011 Yelp
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
from mrjob.job import MRJob
from mrjob.util import get_jobconf_value

class MRJobConfTestOld(MRJob):

    def mapper(self, _, line):
        yield ("mapreduce_job_id", get_jobconf_value("mapred.job.id"))
        yield ("mapreduce_job_local_dir", get_jobconf_value("job.local.dir"))
        yield ("mapreduce_task_id", get_jobconf_value("mapred.tip.id"))
        yield ("mapreduce_task_attempt_id", get_jobconf_value("mapred.task.id")) 
        yield ("mapreduce_task_ismap", get_jobconf_value("mapred.task.is.map"))
        yield ("mapreduce_task_partition", get_jobconf_value("mapred.task.partition"))
        yield ("mapreduce_map_input_file", get_jobconf_value("map.input.file"))
        yield ("mapreduce_map_input_start", get_jobconf_value("map.input.start"))
        yield ("mapreduce_map_input_length", get_jobconf_value("map.input.length"))
        yield ("mapreduce_task_output_dir", get_jobconf_value("mapred.work.output.dir"))
        yield ("mapreduce_job_cache_local_archives", get_jobconf_value("mapred.cache.localArchives"))
        
if __name__ == '__main__':
    MRJobConfTestOld.run()


