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
# limitations under the License.
"""MRWordFreqCount, with a trivial combiner command.

Used to ensure that Spark runner can skip combiners that run commands.
"""
from mrjob.examples.mr_word_freq_count import MRWordFreqCount
from mrjob.job import MRJob


class MRWordFreqCountWithCombinerCmd(MRWordFreqCount):

    combiner = MRJob.combiner  # un-set combiner()

    def combiner_cmd(self):
        return 'cat'


if __name__ == '__main__':
    MRWordFreqCountWithCombinerCmd.run()
