# Copyright 2013 David Marin
# Copyright 2016 Yelp
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
"""A simple example of linking a hadoop jar with a Python step.

This example only works out-of-the-box on EMR; to make it work on Hadoop,
change HADOOP_EXAMPLES_JAR to the (local) path of your hadoop-examples.jar.

This also only works on a single input path/directory, due to limitations
of the example jar.
"""
from mrjob.job import MRJob
from mrjob.protocol import RawProtocol
from mrjob.step import INPUT
from mrjob.step import JarStep
from mrjob.step import MRStep
from mrjob.step import OUTPUT

# use the file:// trick to access a jar hosted on the EMR machines
HADOOP_EXAMPLES_JAR = 'file:///home/hadoop/hadoop-examples.jar'


class MRJarStepExample(MRJob):
    """A contrived example that runs wordcount from the hadoop example
    jar, and then does a frequency count of the frequencies."""

    def steps(self):
        return [
            JarStep(
                jar=HADOOP_EXAMPLES_JAR,
                args=['wordcount', INPUT, OUTPUT]),
            MRStep(
                mapper=self.mapper, combiner=self.reducer,
                reducer=self.reducer)
        ]

    def mapper(self, key, freq):
        yield int(freq), 1

    def reducer(self, freq, counts):
        yield freq, sum(counts)

    def pick_protocols(self, step_num, step_type):
        """Use RawProtocol to read output from the jar."""
        read, write = super(MRJarStepExample, self).pick_protocols(
            step_num, step_type)
        if (step_num, step_type) == (1, 'mapper'):
            read = RawProtocol().read

        return read, write


if __name__ == '__main__':
    MRJarStepExample.run()
