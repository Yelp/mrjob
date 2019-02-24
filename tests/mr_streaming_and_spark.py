# -*- coding: utf-8 -*-
# Copyright 2016 Yelp
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
"""Job that runs (trivial) streaming and spark steps."""
from operator import add

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.step import SparkStep


class MRStreamingAndSpark(MRJob):

    def mapper(self, key, value):
        # converts line to: null\t<JSON of value>
        yield key, value

    def spark(self, input_paths, output_path):
        from pyspark import SparkContext

        sc = SparkContext()

        lines = sc.textFile(input_paths)

        chars = lines.flatMap(lambda line: list(line))
        chars_and_ones = chars.map(lambda c: (c, 1))
        char_counts = chars_and_ones.reduceByKey(add)

        output_lines = char_counts.map(lambda c_n: '%s %d' % c_n)

        output_lines.saveAsTextFile(output_path)

    def steps(self):
        return[
            MRStep(mapper=self.mapper),
            SparkStep(self.spark),
        ]


if __name__ == '__main__':
    MRStreamingAndSpark.run()
