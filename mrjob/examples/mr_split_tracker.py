# Copyright 2017 Yelp
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
"""Don't do anything with the input; just track how Hadoop split it up."""
from mrjob.compat import jobconf_from_env
from mrjob.job import MRJob


class MRSplitTracker(MRJob):

    def configure_options(self):
        super(MRSplitTracker, self).configure_options()

        self.option_parser.add_option(
            '--max-split-size', dest='max_split_size', type=int,
            default=None)

    def jobconf(self):
        return {
            'mapreduce.input.fileinputformat.split.maxsize':
                self.options.max_split_size,
            'mapreduce.input.fileinputformat.split.minsize': 1,
        }

    def mapper_init(self):
        path = jobconf_from_env('mapreduce.map.input.file')
        start = jobconf_from_env('mapreduce.map.input.start')
        length = jobconf_from_env('mapreduce.map.input.length')

        yield path, [start, length]

    def mapper(self, key, value):
        pass


if __name__ == '__main__':
    MRSplitTracker.run()
