# Copyright 2012 Yelp and Contributors
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


class MRTestingJob(MRJob):
    """Simple optimization to make our test cases run faster"""

    def make_runner(self, *args, **kwargs):
        runner = super(MRTestingJob, self).make_runner(*args, **kwargs)
        runner._steps = self._steps_desc()
        return runner
