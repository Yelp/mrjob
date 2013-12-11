# Copyright 2009-2011 Yelp
# Copyright 2013 David Marin
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
"""Trivial multi-step job for testing counter behavior"""
from mrjob.job import MRJob
from mrjob.step import MRStep


class MRCountingJob(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper),
                MRStep(mapper=self.mapper),
                MRStep(mapper=self.mapper)]

    def mapper(self, _, value):
        self.increment_counter('group', 'counter_name', 1)
        yield _, value


if __name__ == '__main__':
    MRCountingJob.run()
