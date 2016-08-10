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
from mrjob.job import MRJob


class MRRunner(MRJob):
    """Print out ``self.option.runner`` in tasks."""

    def configure_options(self):
        super(MRRunner, self).configure_options()

        self.pass_through_option('--runner')

    def mapper(self, key, value):
        return

    def mapper_final(self):
        yield None, self.options.runner


if __name__ == '__main__':
    MRRunner.run()
