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
"""A job that follows the MRJob protocol but doesn't inherit from mrjob.

This counts the number of "words" in a document
"""
import json
import sys

_STEPS = [
    dict(
        type='streaming',
        mapper=dict(type='script'),
        reducer=dict(type='script'),
    ),
]


def main():
    args = sys.argv[1:]

    if '--steps' in args:
        print(json.dumps(_STEPS))
    elif '--mapper' in args:
        for line in sys.stdin:
            print('words\t%d' % len(line.split()))
    elif '--reducer' in args:
        num_words = sum(int(line.split()[-1]) for line in sys.stdin)
        if num_words:  # account for reducer that's passed an empty file
            print(num_words)


if __name__ == '__main__':
    main()
