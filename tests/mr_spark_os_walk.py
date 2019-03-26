# Copyright 2019 Yelp
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
# limitations under the License
from os import walk
from os.path import join
from os.path import getsize

from mrjob.job import MRJob


class MRSparkOSWalk(MRJob):

    def spark(self, input_paths, output_path):
        from pyspark import SparkContext
        sc = SparkContext()

        rdd = sc.parallelize(['.'])
        rdd = rdd.flatMap(self.walk_dir)

        rdd.saveAsTextFile(output_path)

    def walk_dir(self, dirname):
        for dirpath, _, filenames in walk(dirname, followlinks=True):
            for filename in filenames:
                path = join(dirpath, filename)
                size = getsize(path)
                yield path, size


if __name__ == '__main__':
    MRSparkOSWalk.run()
