# Copyright 2012 Yelp
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
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol


class MRFilterJob(MRJob):

    INPUT_PROTOCOL = RawValueProtocol

    INTERNAL_PROTOCOL = RawValueProtocol

    OUTPUT_PROTOCOL = RawValueProtocol

    def configure_options(self):
        super(MRFilterJob, self).configure_options()
        self.add_passthrough_option('--mapper-filter', default=None)
        self.add_passthrough_option('--combiner-filter', default=None)
        self.add_passthrough_option('--reducer-filter', default=None)

    def steps(self):
        kwargs = {}
        if self.options.mapper_filter:
            kwargs['mapper_pre_filter'] = self.options.mapper_filter
        if self.options.combiner_filter:
            kwargs['combiner_pre_filter'] = self.options.combiner_filter
        if self.options.reducer_filter:
            kwargs['reducer_pre_filter'] = self.options.reducer_filter
        return [MRStep(**kwargs)]


if __name__ == '__main__':
    MRFilterJob().run()
