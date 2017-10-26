# Copyright 2011-2012 Yelp
# Copyright 2015-2016 Yelp
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
"""Test the S3 tmpwatch script"""
from datetime import timedelta
import tempfile
import shutil

from mrjob.emr import EMRJobRunner
from mrjob.tools.emr.s3_tmpwatch import _s3_cleanup
from tests.mock_boto3 import MockBoto3TestCase


class S3TmpWatchTestCase(MockBoto3TestCase):

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

        # add some mock data

        # foo is current
        self.add_mock_s3_data(
            {'walrus': {'data/foo': b'foo\n'}})

        # bar and baz are very old (but baz isn't in data/)
        self.add_mock_s3_data(
            {'walrus': {'data/bar': b'bar\n',
                        'other/baz': b'baz\n'}},
            age=timedelta(days=45))

        # qux is a little more than two days old
        self.add_mock_s3_data(
            {'walrus': {'data/qux': b'qux\n'}},
            age=timedelta(hours=50))

        self.assertEqual(
            sorted(runner.fs.ls('s3://walrus/')),
            ['s3://walrus/data/bar', 's3://walrus/data/foo',
             's3://walrus/data/qux', 's3://walrus/other/baz'],
        )

        # try a dry run, which shouldn't delete anything
        _s3_cleanup('s3://walrus/data/', timedelta(days=30), dry_run=True,
                    conf_paths=[])

        self.assertEqual(
            sorted(runner.fs.ls('s3://walrus/')), [
                's3://walrus/data/bar',
                's3://walrus/data/foo',
                's3://walrus/data/qux',
                's3://walrus/other/baz',
            ],
        )
        # now do it for real. should hit bar (baz isn't in data/)
        _s3_cleanup('s3://walrus/data', timedelta(days=30), conf_paths=[])

        self.assertEqual(
            sorted(runner.fs.ls('s3://walrus/')), [
                's3://walrus/data/foo',
                's3://walrus/data/qux',
                's3://walrus/other/baz',
            ],
        )

        # now try to delete qux too
        _s3_cleanup('s3://walrus/data', timedelta(hours=48), conf_paths=[])

        self.assertEqual(
            sorted(runner.fs.ls('s3://walrus/')), [
                's3://walrus/data/foo',
                's3://walrus/other/baz',
            ],
        )
