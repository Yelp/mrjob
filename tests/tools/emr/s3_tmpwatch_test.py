# Copyright 2011 Yelp
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

from __future__ import with_statement

from datetime import datetime, timedelta
import os
import tempfile
from testify import TestCase, assert_equal, setup, teardown
import shutil

try:
    import boto
    import boto.utils
    from mrjob import botoemr
except ImportError:
    boto = None
    botoemr = None

from mrjob.conf import dump_mrjob_conf
from mrjob.tools.emr.s3_tmpwatch import *
from tests.emr_test import MockEMRAndS3TestCase
from tests.mockboto import MockKey

class S3TmpWatchTestCase(MockEMRAndS3TestCase):

    @setup
    def make_tmp_dir_and_mrjob_conf(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.mrjob_conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')
        dump_mrjob_conf({'runners': {'emr': {
            'check_emr_status_every': 0.01,
            's3_sync_wait_time': 0.01,
        }}}, open(self.mrjob_conf_path, 'w'))

    @teardown
    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def test_cleanup(self):
        runner = EMRJobRunner(conf_path=False, s3_sync_wait_time=0.01)
        
        # add some mock data and change last_modified
        remote_input_path = 's3://walrus/data/'
        self.add_mock_s3_data({'walrus': {'data/foo': 'foo\n', 
                                        'data/bar': 'bar\n',
                                        'data/qux': 'qux\n'}})
        
        s3_conn = runner.make_s3_conn()
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
            
        s3_cleanup(remote_input_path, timedelta(days=30), dry_run=True, conf_path=False)
        
        # dry-run shouldn't delete anything
        assert isinstance(key_foo, MockKey)
        assert isinstance(key_bar, MockKey)
        assert isinstance(key_qux, MockKey)
        
        s3_cleanup(remote_input_path, timedelta(days=30), conf_path=False)
        
        key_foo = bucket.get_key('data/foo')
        key_bar = bucket.get_key('data/bar')
        key_qux = bucket.get_key('data/qux')
        
        # make sure key_bar is deleted
        assert isinstance(key_foo, MockKey)
        assert_equal(key_bar, None)
        assert isinstance(key_qux, MockKey)
        
        s3_cleanup(remote_input_path, timedelta(hours=48), conf_path=False)
        
        key_foo = bucket.get_key('data/foo')
        key_bar = bucket.get_key('data/bar')
        key_qux = bucket.get_key('data/qux')
        
        # make sure key_qux is deleted
        assert isinstance(key_foo, MockKey)
        assert_equal(key_bar, None)
        assert_equal(key_qux, None)
   