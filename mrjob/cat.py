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
"""A simple utility that prints the contents of files, uncompressing
files based on their file extension.

This does not handle globs."""
from sys import argv
from sys import stdout

from mrjob.util import read_input


def main():
    # we want to write bytes
    if hasattr(stdout, 'buffer'):
        stdout_buffer = stdout.buffer
    else:
        stdout_buffer = stdout

    for path in argv[1:] or ['-']:
        for chunk in read_input(path):
            stdout_buffer.write(chunk)


if __name__ == '__main__':
    main()
