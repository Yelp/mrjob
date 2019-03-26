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
from os import getcwd
from os import walk
from os.path import join
from os.path import getsize
from os.path import relpath

from mrjob.job import MRJob


class MRSparkOSWalk(MRJob):

    def configure_args(self):
        super(MRSparkOSWalk, self).configure_args()

        self.add_passthru_arg(
            '--use-executor-cwd', dest='use_executor_cwd',
            default=False, action='store_true')

    def spark(self, input_paths, output_path):
        from pyspark import SparkContext
        sc = SparkContext()

        # we'd like to walk the executor's directory. However, when running
        # directly through pyspark (inline runner), there's no way to restart
        # the JVM with a new CWD, so instead we walk the executor's directory
        # (which drivers are *supposed* to match, if they have a fresh JVM)
        if self.options.use_executor_cwd:
            cwd = getcwd()
        else:
            cwd = '.'

        rdd = sc.parallelize([cwd])
        rdd = rdd.flatMap(self.walk_dir)

        rdd.saveAsTextFile(output_path)

    def walk_dir(self, root_dir):
        for dirpath, _, filenames in walk(root_dir, followlinks=True):
            for filename in filenames:
                path = join(dirpath, filename)
                size = getsize(path)
                yield relpath(path, root_dir), size


if __name__ == '__main__':
    MRSparkOSWalk.run()
