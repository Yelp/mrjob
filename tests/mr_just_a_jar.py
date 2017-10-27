# -*- coding: utf-8 -*-
# Copyright 2009-2013 Yelp and Contributors
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
"""Job that runs a single jar, specified on the command line."""

from mrjob.job import MRJob
from mrjob.step import JarStep


class MRJustAJar(MRJob):

    def configure_args(self):
        super(MRJustAJar, self).configure_args()

        self.add_passthru_arg('--jar')
        self.add_passthru_arg('--main-class', default=None)

    def steps(self):
        return [JarStep(jar=self.options.jar,
                        main_class=self.options.main_class)]


if __name__ == '__main__':
    MRJustAJar.run()
