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
from io import BytesIO
from os.path import join

from warcio.warcwriter import WARCWriter

from mrjob.examples.mr_phone_to_url import MRPhoneToURL

from tests.sandbox import SandboxedTestCase
from tests.job import run_job


class MRPhoneToURLTestCase(SandboxedTestCase):

    def test_empty(self):
        self.assertEqual(run_job(MRPhoneToURL()), {})

    def test_three_pages(self):
        wet1 = BytesIO()
        writer1 = WARCWriter(wet1, gzip=False)

        write_conversion_record(
            writer1, 'https://nophonenumbershere.info',
            b'THIS-IS-NOT-A-NUMBER')
        write_conversion_record(
            writer1, 'https://big.directory/',
            b'The Time: (612) 777-9311\nJenny: (201) 867-5309\n')

        wet2_gz_path = join(self.tmp_dir, 'wet2.warc.wet.gz')
        with open(wet2_gz_path, 'wb') as wet2:
            writer2 = WARCWriter(wet2, gzip=True)

            write_conversion_record(
                writer2, 'https://jseventplanning.biz/',
                b'contact us at +1 201 867 5309')

        self.assertEqual(
            run_job(MRPhoneToURL([wet2_gz_path, '-']),
                    raw_input=wet1.getvalue()),
            {'+12018675309': 'https://jseventplanning.biz/',
             '+16127779311': 'https://big.directory/'})


def write_conversion_record(writer, url, content):
    writer.write_record(writer.create_warc_record(
        url, 'conversion', warc_content_type='text/plain',
        payload=BytesIO(content)))
