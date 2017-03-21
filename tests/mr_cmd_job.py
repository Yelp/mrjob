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
from mrjob.protocol import RawValueProtocol
from mrjob.step import MRStep


class MRCmdJob(MRJob):

    INPUT_PROTOCOL = RawValueProtocol

    INTERNAL_PROTOCOL = RawValueProtocol

    OUTPUT_PROTOCOL = RawValueProtocol

    def configure_options(self):
        super(MRCmdJob, self).configure_options()
        self.add_passthrough_option('--mapper-cmd', default=None)
        self.add_passthrough_option('--combiner-cmd', default=None)
        self.add_passthrough_option('--reducer-cmd', default=None)
        self.add_passthrough_option('--reducer-cmd-2', default=None)

    def steps(self):
        kwargs = {}
        if self.options.mapper_cmd:
            kwargs['mapper_cmd'] = self.options.mapper_cmd
        if self.options.combiner_cmd:
            kwargs['combiner_cmd'] = self.options.combiner_cmd
        if self.options.reducer_cmd:
            kwargs['reducer_cmd'] = self.options.reducer_cmd
        steps = [MRStep(**kwargs)]

        if self.options.reducer_cmd_2:
            steps.append(MRStep(reducer_cmd=self.options.reducer_cmd_2))

        return steps


if __name__ == '__main__':
    MRCmdJob().run()
