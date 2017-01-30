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
"""Job to test if runners respect SORT_VALUES."""
from mrjob.job import MRJob
from mrjob.step import MRStep


class MRSortValuesAndMore(MRJob):

    JOBCONF = {
        'mapreduce.partition.keycomparator.options': '-k1 -k2nr',
        'stream.num.map.output.key.fields': 3,
    }

    PARTITIONER = 'org.apache.hadoop.mapred.lib.HashPartitioner'

    SORT_VALUES = True

    def dummy_mapper_init(self):
        pass

    def steps(self):
        return [
            MRStep(
                mapper_init=self.dummy_mapper_init,
            ),
            MRStep(
                mapper_init=self.dummy_mapper_init,
                jobconf={
                    'mapreduce.job.output.key.comparator.class':
                    'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                }
            ),
        ]


if __name__ == '__main__':
    MRSortValuesAndMore.run()
