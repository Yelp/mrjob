# Copyright 2013 David Marin
# Copyright 2016 Yelp
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
from mrjob.job import MRJob
from mrjob.step import INPUT
from mrjob.step import JarStep
from mrjob.step import MRStep
from mrjob.step import OUTPUT


class MRJarAndStreaming(MRJob):

    def configure_args(self):
        super(MRJarAndStreaming, self).configure_args()

        self.add_passthru_arg('--jar')

    def steps(self):
        return [
            JarStep(
                jar=self.options.jar,
                args=['stuff', INPUT, OUTPUT]
            ),
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]

    def mapper(self, key, value):
        pass

    def reducer(self, key, value):
        pass


if __name__ == '__main__':
    MRJarAndStreaming.run()
