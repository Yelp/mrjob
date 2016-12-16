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
import re
from operator import add

from mrjob.job import MRJob

# restrict to a-z so we don't get odd filenames
WORD_RE = re.compile(r"[A-Za-z]+")


class MRSparkNickNackWordCount(MRJob):
    # use the nicknack JAR in this directory
    #
    # This JAR was downloaded from https://github.com/empiricalresults/nicknack
    # and is also under the Apache 2.0 license.
    LIBJARS = ['nicknack-1.0.0.jar']

    def spark(self, input_path, output_path):
        # Spark may not be available where script is launched
        from pyspark import SparkContext

        sc = SparkContext(appName='mrjob Spark wordcount script')

        lines = sc.textFile(input_path)

        counts = (
            lines.flatMap(lambda line: WORD_RE.findall(line))
            .map(lambda word: (word, 1))
            .reduceByKey(add))

        # MultipleValueOutputFormat expects Text, Text
        # w_c is (word, count)
        counts = counts.map(lambda w_c: (w_c[0], str(w_c[1])))

        counts.saveAsHadoopFile(output_path,
                                'nicknack.MultipleValueOutputFormat')

        sc.stop()


if __name__ == '__main__':
    MRSparkNickNackWordCount.run()
