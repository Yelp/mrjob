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
"""Job that runs a single spark script, specified on the command line."""

from mrjob.job import MRJob
from mrjob.step import SparkScriptStep


class MRSparkScript(MRJob):

    def configure_options(self):
        super(MRSparkScript, self).configure_options()

        self.add_passthrough_option(
            '--script', dest='script')
        self.add_passthrough_option(
            '--script-arg', dest='script_args',
            action='append', default=[])
        self.add_passthrough_option(
            '--script-spark-arg', dest='script_spark_args',
            action='append', default=[])

    def steps(self):
        return [SparkScriptStep(
            script=self.options.script,
            args=self.options.script_args,
            spark_args=self.options.script_spark_args,
        )]


if __name__ == '__main__':
    MRSparkScript.run()
