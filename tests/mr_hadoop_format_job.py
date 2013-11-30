# Copyright 2009-2011 Yelp
# Copyright 2012 Yelp and Contributors
# Copyright 2013 David Marin
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
"""Trivial two-step job which sets hadoop input and output format."""
from mrjob.job import MRJob
from mrjob.step import MRStep


class MRHadoopFormatJob(MRJob):

    HADOOP_INPUT_FORMAT = 'mapred.FooInputFormat'
    HADOOP_OUTPUT_FORMAT = 'mapred.BarOutputFormat'

    def mapper(self, key, value):
        yield key, value
        yield value, key

    def combiner(self, key, values):
        # just pass through and make note that this was run
        self.increment_counter('count', 'combiners', 1)
        for value in values:
            yield key, value

    def reducer(self, key, values):
        yield key, len(list(values))

    def mapper2(self, key, value):
        yield value, key

    def steps(self):
        return [MRStep(mapper=self.mapper,
                       combiner=self.combiner,
                       reducer=self.reducer),
                MRStep(mapper=self.mapper2, jobconf={'x': 'z'})]


if __name__ == '__main__':
    MRHadoopFormatJob.run()
