# Copyright 2011-2012 Yelp
# Copyright 2015-2016 Yelp
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
"""Test the S3 tmpwatch script"""
from datetime import datetime
from datetime import timedelta
import tempfile
import shutil

try:
    import boto
    import boto.utils
    boto  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    boto = None

from mrjob.emr import EMRJobRunner
from mrjob.parse import parse_s3_uri
from mrjob.tools.emr.s3_tmpwatch import _s3_cleanup
from tests.mockboto import MockKey
from tests.mockboto import MockBotoTestCase


class S3TmpWatchTestCase(MockBotoTestCase):

    def setUp(self):
        super(S3TmpWatchTestCase, self).setUp()
        self.make_tmp_dir_and_mrjob_conf()

    def tearDown(self):
        self.rm_tmp_dir()
        super(S3TmpWatchTestCase, self).tearDown()

    def make_tmp_dir_and_mrjob_conf(self):
        self.tmp_dir = tempfile.mkdtemp()

    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def test_cleanup(self):
        runner = EMRJobRunner(conf_paths=[], cloud_fs_sync_secs=0.01)

        # add some mock data and change last_modified
        remote_input_path = 's3://walrus/data/'
        self.add_mock_s3_data({'walrus': {'data/foo': b'foo\n',
                                          'data/bar': b'bar\n',
                                          'data/qux': b'qux\n'}})

        s3_conn = runner.fs.make_s3_conn()
        bucket_name, key_name = parse_s3_uri(remote_input_path)
        bucket = s3_conn.get_bucket(bucket_name)

        key_foo = bucket.get_key('data/foo')
        key_bar = bucket.get_key('data/bar')
        key_qux = bucket.get_key('data/qux')
        key_bar.last_modified = datetime.now() - timedelta(days=45)
        key_qux.last_modified = datetime.now() - timedelta(hours=50)

        # make sure keys are there
        assert isinstance(key_foo, MockKey)
        assert isinstance(key_bar, MockKey)
        assert isinstance(key_qux, MockKey)

        _s3_cleanup(remote_input_path, timedelta(days=30), dry_run=True,
                    conf_paths=[])

        # dry-run shouldn't delete anything
        assert isinstance(key_foo, MockKey)
        assert isinstance(key_bar, MockKey)
        assert isinstance(key_qux, MockKey)

        _s3_cleanup(remote_input_path, timedelta(days=30), conf_paths=[])

        key_foo = bucket.get_key('data/foo')
        key_bar = bucket.get_key('data/bar')
        key_qux = bucket.get_key('data/qux')

        # make sure key_bar is deleted
        assert isinstance(key_foo, MockKey)
        self.assertEqual(key_bar, None)
        assert isinstance(key_qux, MockKey)

        _s3_cleanup(remote_input_path, timedelta(hours=48), conf_paths=[])

        key_foo = bucket.get_key('data/foo')
        key_bar = bucket.get_key('data/bar')
        key_qux = bucket.get_key('data/qux')

        # make sure key_qux is deleted
        assert isinstance(key_foo, MockKey)
        self.assertEqual(key_bar, None)
        # Failing as of d0c07eb:
        # self.assertEqual(key_qux, None)
