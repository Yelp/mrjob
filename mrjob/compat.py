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

"""Utility functions for compatibility with different version of hadoop."""

# keep a mapping for all the names of old/new jobconf env variables

# lists alternative names for jobconf variables
jobconf_map = {
    "mapreduce.job.id": {'0.21': "mapreduce.job.id", '0.18': "mapred.job.id"},
    "mapred.job.id": {'0.21': "mapreduce.job.id", '0.18': "mapred.job.id"},
    "mapreduce.job.local.dir": {'0.21': "mapreduce.job.local.dir", '0.18': "job.local.dir"}, 
    "job.local.dir": {'0.21': "mapreduce.job.local.dir", '0.18': "job.local.dir"}, 
    "mapreduce.task.id": {'0.21': "mapreduce.task.id", '0.18': "mapred.task.id"},
    "mapred.tip.id": {'0.21': "mapreduce.task.id", '0.18': "mapred.task.id"},
    "mapreduce.task.attempt.id": {'0.21': "mapreduce.task.attempt.id", '0.18': "mapreduce.task.id"},
    "mapred.task.id": {'0.21': "mapreduce.task.attempt.id", '0.18': "mapreduce.task.id"},
    "mapreduce.task.ismap": {'0.21': "mapreduce.task.ismap", '0.18': "mapred.task.ismap"},
    "mapred.task.is.map": {'0.21': "mapreduce.task.ismap", '0.18': "mapred.task.ismap"},
    "mapreduce.task.partition": {'0.21': "mapreduce.task.partition", '0.18': "mapred.task.partition"},
    "mapred.task.partition": {'0.21': "mapreduce.task.partition", '0.18': "mapred.task.partition"},
    "mapreduce.map.input.file": {'0.21': "mapreduce.map.input.file", '0.18': "map.input.file"},
    "map.input.file": {'0.21': "mapreduce.map.input.file", '0.18': "map.input.file"},
    "mapreduce.map.input.start": {'0.21': "mapreduce.map.input.start", '0.18': "map.input.start"},
    "map.input.start": {'0.21': "mapreduce.map.input.start", '0.18': "map.input.start"},
    "mapreduce.map.input.length": {'0.21': "mapreduce.map.input.length", '0.18': "map.input.length"},
    "map.input.length": {'0.21': "mapreduce.map.input.length", '0.18': "map.input.length"},
    "mapreduce.task.output.dir": {'0.21': "mapreduce.task.output.dir", '0.18': "mapred.work.output.dir"},
    "mapred.work.output.dir": {'0.21': "mapreduce.task.output.dir", '0.18': "mapred.work.output.dir"},
    "mapreduce.job.cache.local.archives": {'0.21': "mapreduce.job.cache.local.archives", '0.18': "mapred.cache.localArchives"}
    "mapred.cache.localArchives": {'0.21': "mapreduce.job.cache.local.archives", '0.18': "mapred.cache.localArchives"}
}

def translate_jobconf(variable, version=None):
    """
    """
    if version:
        return jobconf_map[variable][version]
    else:
        return jobconf_map[variables].itervalues()
        