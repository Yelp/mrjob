# Copyright 2015 Yelp
# Copyright 2017 Yelp
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
"""An example of how to parse non-line-based data.

Map +1 (U.S. and Canadian) phone numbers to the most plausible
URL for their webpage.

This is similar to the article "Analyzing the Web for the Price of a Sandwich"
(https://engineeringblog.yelp.com/2015/03/analyzing-the-web-for-the-price-of-a-sandwich.html)
except that it doesn't include Yelp biz IDs, and it doesn't need to access
S3 because it can read the input files directly.

Sample command line:

.. code-block:: sh

   python mr_phone_to_url.py -r emr --bootstrap 'sudo pip install warc' s3://commoncrawl/crawl-data/CC-MAIN-2017-51/segments/*/wet/*.wet.gz

To find the latest crawl:

``aws s3 ls s3://commoncrawl/crawl-data | grep CC-MAIN``
"""
import os
import re
import sys
from bz2 import BZ2File
from contextlib import contextmanager
from gzip import GzipFile
from itertools import islice
from subprocess import Popen
from subprocess import PIPE

from mrjob.job import MRJob
from mrjob.parse import is_uri
from mrjob.protocol import RawProtocol
from mrjob.py2 import urlparse
from mrjob.step import MRStep
from mrjob.util import cmd_line
from mrjob.util import random_identifier

PHONE_RE = re.compile(
    r'[\D\b](1?[2-9]\d{2}[\-. ()+]+\d{3}[\-. ()+]+\d{4})[\D\b]')
PHONE_SEP_RE = re.compile(r'[\-. ()+]')

# hosts with more than this many phone numbers are assumed to be directories
MAX_PHONES_PER_HOST = 1000


def standardize_phone_number(number):
    number_sep = PHONE_SEP_RE.split(number)
    number = "".join(number_sep)
    if len(number) > 7:
        if number[-1] not in '0123456789':
            number = number[:-1]
        if number[0] not in '0123456789':
            number = number[1:]
    if len(number) <= 10:
        return "+1" + number
    else:
        return "+" + number


class MRPhoneToURL(MRJob):
    """Use Common Crawl .wet files to map from phone number to the most
    likely URL."""

    HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.NLineInputFormat'
    INPUT_PROTOCOL = RawProtocol

    def steps(self):
        return [
            MRStep(mapper=self.extract_phone_and_url_mapper,
                   reducer=self.count_by_host_reducer),
            MRStep(reducer=self.pick_best_url_reducer),
        ]

    def extract_phone_and_url_mapper(self, record_num, path):
        """Read .wet from ``self.stdin`` (so we can read multi-line records).

        Yield ``host, (phone, url)``
        """
        import warc

        uri = 's3://commoncrawl/' + path

        # misconfigured hadoop environment on EMR
        for k, v in sorted(os.environ.items()):
            sys.stderr.write('%s=%s\n' % (k, v))

        with download_input(uri) as path:
            with open_input(path) as f:
                wet_file = warc.WARCFile(fileobj=f)

                for record in wet_file:
                    if record['content-type'] != 'text/plain':
                        continue

                    url = record.header['warc-target-uri']
                    host = urlparse(url).netloc

                    payload = record.payload.read()
                    for phone in PHONE_RE.findall(payload):
                        phone = standardize_phone_number(phone)
                        yield host, (phone, url)

    def count_by_host_reducer(self, host, phone_urls):
        phone_urls = list(islice(phone_urls, MAX_PHONES_PER_HOST + 1))

        # don't bother with directories, etc.
        host_phone_count = len(phone_urls)
        if host_phone_count > MAX_PHONES_PER_HOST:
            return

        for phone, url in phone_urls:
            yield phone, (url, host_phone_count)

    def pick_best_url_reducer(self, phone, urls_with_count):
        # pick the url that appears on a host with the least number of
        # phone numbers, breaking ties by choosing the shortest URL
        # and the one that comes first alphabetically
        urls_with_count = sorted(
            urls_with_count, key=lambda uc: (-uc[1], -len(uc[0]), uc[0]))

        yield phone, urls_with_count[0][0]


@contextmanager
def download_input(uri):
    if not is_uri(uri):
        yield uri
        return

    path = '%s-%s' % (random_identifier(), uri.split('/')[-1])

    #cp_args = ['hadoop', 'fs', '-copyToLocal', uri, path]
    cp_args = ['true']
    env = {
        k: v for k, v in os.environ.items()
        if not k.startswith('BASH_FUNC_')
    }

    try:
        sys.stderr.write('> ' + cmd_line(cp_args) + '\n')

        cp_proc = Popen(cp_args, stdin=PIPE, stdout=PIPE, stderr=PIPE, env=env)

        sys.stderr.write('Popened\n')

        stdout, stderr = cp_proc.communicate()

        sys.stderr.write('communicated\n')

        if stderr:
            stderr_buffer = getattr(sys.stderr, 'buffer', sys.stderr)
            stderr_buffer.write(stderr + b'\n')

        if cp_proc.returncode != 0:
            sys.stderr.write(
                'returned nonzero error status: %d' % cp_proc.returncode)
        else:
            sys.stderr.write('copied to %s\n' % path)

    finally:
        try:
            os.remove(path)
        except OSError:
            pass

    yield path



def open_input(path):
    if path.endswith('.gz'):
        return GzipFile(path, 'rb')
    elif path.endswith('.bz2'):
        return BZ2File(path, 'rb')
    else:
        return open(path, 'rb')




if __name__ == '__main__':
    MRPhoneToURL.run()
