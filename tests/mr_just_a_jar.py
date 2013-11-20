# -*- coding: utf-8 -*-
# Copyright 2009-2013 Yelp and Contributors
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
"""Job that runs a single jar, specified on the command line."""

from mrjob.job import MRJob
from mrjob.step import JarStep


class MRJustAJar(MRJob):

    def configure_options(self):
        super(MRJustAJar, self).configure_options()

        self.add_passthrough_option('--jar')

    def steps(self):
        return [JarStep(jar=self.options.jar)]


if __name__ == '__main__':
    MRJustAJar.run()
