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
# limitations under the License.from os.path import join
"""Silly submodule for a silly library"""

def encode(s):
    def encode_char(c):
        if 'A' <= c <='M' or 'a' <= c <= 'm':
            return chr(ord(c) + 13)
        elif 'N' <= c <= 'Z' or 'n' <= c <= 'z':
            return chr(ord(c) - 13)
        else:
            return c

    return ''.join(encode_char(c) for c in s)
