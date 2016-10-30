# -*- coding: utf-8 -*-
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
"""Job that runs an empty spark() method."""

from mrjob.job import MRJob


class MRNullSpark(MRJob):

    def configure_options(self):
        super(MRNullSpark, self).configure_options()

        self.add_passthrough_option(
            '--extra-spark-arg', dest='extra_spark_args',
            action='append', default=[])

        self.add_file_option(
            '--extra-file', dest='extra_file')

    def spark(self):
        pass

    def spark_args(self):
        return self.options.extra_spark_args


if __name__ == '__main__':
    MRNullSpark.run()
