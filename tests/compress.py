# Copyright 2013 David Marin
# Copyright 2015 Yelp
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
"""Utilities to compress data in memory."""
import gzip
from io import BytesIO


# use bz2.compress() to compress bz2 data

def gzip_compress(data):
    """return the gzip-compressed version of the given bytes."""
    s = BytesIO()
    g = gzip.GzipFile(fileobj=s, mode='wb')
    g.write(data)
    g.close()
    return s.getvalue()
