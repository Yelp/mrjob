# Copyright 2018 Yelp
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


class MRRot13Lib(MRJob):
    """A job that uses DIRS to import a library."""

    DIRS = ['rot13lib']

    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, _, line):
        # . isn't in sys.path on EMR for some reason
        import sys
        sys.path.append('.')
        try:
            from rot13lib.text import encode

            rot13_line = encode(line)
        finally:
            sys.path.pop()

        yield None, encode(line)


if __name__ == '__main__':
    MRRot13Lib.run()
