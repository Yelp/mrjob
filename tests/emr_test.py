# Copyright 2009-2010 Yelp
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
"""Unit testing for EMRJobRunner"""
from __future__ import with_statement

import boto
from StringIO import StringIO
import os
import shutil
import tempfile
from testify import TestCase, assert_equal, assert_raises, setup, teardown

from mrjob import botoemr

from mrjob.conf import dump_mrjob_conf
from mrjob.emr import EMRJobRunner
from tests.mockboto import MockS3Connection, MockEmrConnection, add_mock_s3_data
from tests.mr_two_step_job import MRTwoStepJob

class MockEMRAndS3TestCase(TestCase):

    @setup
    def make_mrjob_conf(self):
        _, self.mrjob_conf_path = tempfile.mkstemp(prefix='mrjob.conf.')
        dump_mrjob_conf({'runners': {'emr': {
            'check_emr_status_every': 0.01,
            's3_scratch_uri': 's3://walrus/tmp',
            's3_sync_wait_time': 0.01,
        }}}, open(self.mrjob_conf_path, 'w'))

    @setup
    def rm_mrjob_conf(self):
        os.unlink(self.mrjob_conf_path)

    @setup
    def sandbox_boto(self):
        self.mock_s3_fs = {}
        self.mock_emr_job_flows = {}
        self.mock_emr_failures = {}
        self.mock_emr_output = {}

        def mock_boto_connect_s3(*args, **kwargs):
            kwargs['mock_s3_fs'] = self.mock_s3_fs
            return MockS3Connection(*args, **kwargs)

        def mock_botoemr_EmrConnection(*args, **kwargs):
            kwargs['mock_s3_fs'] = self.mock_s3_fs
            kwargs['mock_emr_job_flows'] = self.mock_emr_job_flows
            kwargs['mock_emr_failures'] = self.mock_emr_failures
            kwargs['mock_emr_output'] = self.mock_emr_output
            return MockEmrConnection(*args, **kwargs)
        
        self._real_boto_connect_s3 = boto.connect_s3
        boto.connect_s3 = mock_boto_connect_s3
        
        self._real_botoemr_EmrConnection = botoemr.EmrConnection
        botoemr.EmrConnection = mock_botoemr_EmrConnection

    @teardown
    def unsandbox_boto(self):
        boto.connect_s3 = self._real_boto_connect_s3
        botoemr.EmrConnection = self._real_botoemr_EmrConnection

    def add_mock_s3_data(self, data):
        """Update self.mock_s3_fs with a map from bucket name
        to key name to data."""
        add_mock_s3_data(self.mock_s3_fs, data)

class EMRJobRunnerEndToEndTestCase(MockEMRAndS3TestCase):

    @setup
    def make_tmp_dir_and_mrjob_conf(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.mrjob_conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')
        dump_mrjob_conf({'runners': {'emr': {
            'check_emr_status_every': 0.01,
            's3_scratch_uri': 's3://walrus/tmp',
            's3_sync_wait_time': 0.01,
        }}}, open(self.mrjob_conf_path, 'w'))

    @teardown
    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def test_end_to_end(self):
        # read from STDIN, a local file, and a remote file
        stdin = StringIO('foo\nbar\n')

        local_input_path = os.path.join(self.tmp_dir, 'input')
        with open(local_input_path, 'w') as local_input_file:
            local_input_file.write('bar\nqux\n')

        remote_input_path = 's3://walrus/data/foo'
        self.add_mock_s3_data({'walrus': {'data/foo': 'foo\n'}})

        # setup fake output
        self.mock_emr_output = {('j-MOCKJOBFLOW0', 1): [
            '1\t"qux"\n2\t"bar"\n', '2\t"foo"\n5\tnull\n']}

        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '-c', self.mrjob_conf_path,
                               '-', local_input_path, remote_input_path])
        mr_job.sandbox(stdin=stdin)

        local_tmp_dir = None
        results = []

        with mr_job.make_runner() as runner:
            assert isinstance(runner, EMRJobRunner)
            runner.run()

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

            local_tmp_dir = runner._get_local_tmp_dir()
            # make sure cleanup hasn't happened yet
            assert os.path.exists(local_tmp_dir)
            assert any(runner.ls(runner.get_output_dir()))

            emr_conn = runner.make_emr_conn()
            job_flow = emr_conn.describe_jobflow(runner.get_emr_job_flow_id())
            assert_equal(job_flow.state, 'COMPLETED')

        assert_equal(sorted(results),
                     [(1, 'qux'), (2, 'bar'), (2, 'foo'), (5, None)])

        # make sure cleanup happens
        assert not os.path.exists(local_tmp_dir)
        assert not any(runner.ls(runner.get_output_dir()))

        # job should get terminated
        emr_conn = runner.make_emr_conn()
        job_flow_id = runner.get_emr_job_flow_id()
        for i in range(10):
            emr_conn.simulate_progress(job_flow_id)

        job_flow = emr_conn.describe_jobflow(job_flow_id)
        assert_equal(job_flow.state, 'TERMINATED')

    def test_failed_job(self):
        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '-c', self.mrjob_conf_path])
        mr_job.sandbox()

        self.add_mock_s3_data({'walrus': {}})
        self.mock_emr_failures = {('j-MOCKJOBFLOW0', 0): None}

        with mr_job.make_runner() as runner:
            assert isinstance(runner, EMRJobRunner)

            assert_raises(Exception, runner.run)

            emr_conn = botoemr.EmrConnection()
            job_flow_id = runner.get_emr_job_flow_id()
            for i in range(10):
                emr_conn.simulate_progress(job_flow_id)

            job_flow = emr_conn.describe_jobflow(job_flow_id)
            assert_equal(job_flow.state, 'FAILED')

        # job should get terminated on cleanup
        emr_conn = runner.make_emr_conn()
        job_flow_id = runner.get_emr_job_flow_id()
        for i in range(10):
            emr_conn.simulate_progress(job_flow_id)

        job_flow = emr_conn.describe_jobflow(job_flow_id)
        assert_equal(job_flow.state, 'TERMINATED')

    def test_pick_s3_scratch_uri(self):
        self.add_mock_s3_data({'walrus': {}, 'zebra': {}})
        runner = EMRJobRunner(conf_path=False)

        assert_equal(runner._opts['s3_scratch_uri'],
                     's3://walrus/tmp/mrjob/')

    def test_cant_pick_s3_scratch_uri_with_no_buckets(self):
        assert_raises(Exception, EMRJobRunner, conf_path=False)

### tests for error parsing ###

BUCKET = 'walrus'
BUCKET_URI = 's3://' + BUCKET + '/'

LOG_DIR = 'j-JOBFLOWID/'

GARBAGE = \
"""GarbageGarbageGarbage
"""

TRACEBACK_START = 'Traceback (most recent call last):\n'

PY_EXCEPTION = \
"""  File "<string>", line 1, in <module>
TypeError: 'int' object is not iterable
"""

CHILD_ERR_LINE = '2010-07-27 18:25:48,397 WARN org.apache.hadoop.mapred.TaskTracker (main): Error running child\n'

JAVA_STACK_TRACE = """java.lang.OutOfMemoryError: Java heap space
        at org.apache.hadoop.mapred.IFile$Reader.readNextBlock(IFile.java:270)
        at org.apache.hadoop.mapred.IFile$Reader.next(IFile.java:332)
"""

HADOOP_ERR_LINE_PREFIX = '2010-07-27 19:53:35,451 ERROR org.apache.hadoop.streaming.StreamJob (main): '

USEFUL_HADOOP_ERROR = 'Error launching job , Output path already exists : Output directory s3://yourbucket/logs/2010/07/23/ already exists and is not empty'

BORING_HADOOP_ERROR = 'Job not Successful!'
TASK_ATTEMPTS_DIR = LOG_DIR + 'task-attempts/'

ATTEMPT_DIR = TASK_ATTEMPTS_DIR + 'attempt_201007271720_0001_m_000126_0/'

def make_input_uri_line(input_uri):
    return "2010-07-27 17:55:29,400 INFO org.apache.hadoop.fs.s3native.NativeS3FileSystem (main): Opening '%s' for reading\n" % input_uri

class FindProbableCauseOfFailureTestCase(MockEMRAndS3TestCase):
    # We're mostly concerned here that the right log files are read in the
    # right order. parsing of the logs is handled by tests.parse_test

    @setup
    def make_runner(self):
        self.runner = EMRJobRunner(s3_sync_wait_time=0,
                                   s3_scratch_uri='s3://walrus/tmp',
                                   conf_path=False)
        self.runner._s3_job_log_uri = BUCKET_URI + LOG_DIR

    @teardown
    def cleanup_runner(self):
        self.runner.cleanup()

    def test_empty(self):
        self.add_mock_s3_data({'walrus': {}})
        assert_equal(self.runner._find_probable_cause_of_failure([1]), None)

    def test_python_exception(self):
        self.add_mock_s3_data({'walrus': {
            ATTEMPT_DIR + 'stderr': 
                GARBAGE + TRACEBACK_START + PY_EXCEPTION + GARBAGE,
            ATTEMPT_DIR + 'syslog': 
                make_input_uri_line(BUCKET_URI + 'input.gz'),
        }})
        assert_equal(self.runner._find_probable_cause_of_failure([1]),
                     {'lines': list(StringIO(PY_EXCEPTION)),
                      's3_log_file_uri': 
                          BUCKET_URI + ATTEMPT_DIR + 'stderr',
                      'input_uri': BUCKET_URI + 'input.gz'})

    def test_python_exception_without_input_uri(self):
        self.add_mock_s3_data({'walrus': {
            ATTEMPT_DIR + 'stderr': (
                GARBAGE + TRACEBACK_START + PY_EXCEPTION + GARBAGE),
        }})
        assert_equal(self.runner._find_probable_cause_of_failure([1]),
                     {'lines': list(StringIO(PY_EXCEPTION)),
                      's3_log_file_uri': 
                          BUCKET_URI + ATTEMPT_DIR + 'stderr',
                      'input_uri': None})

    def test_java_exception(self):
        self.add_mock_s3_data({'walrus': {
            ATTEMPT_DIR + 'stderr': GARBAGE + GARBAGE,
            ATTEMPT_DIR + 'syslog': 
                make_input_uri_line(BUCKET_URI + 'input.gz') +
                GARBAGE +
                CHILD_ERR_LINE +
                JAVA_STACK_TRACE +
                GARBAGE,
        }})
        assert_equal(self.runner._find_probable_cause_of_failure([1]),
                     {'lines': list(StringIO(JAVA_STACK_TRACE)),
                      's3_log_file_uri': 
                          BUCKET_URI + ATTEMPT_DIR + 'syslog',
                      'input_uri': BUCKET_URI + 'input.gz'})

    def test_java_exception_without_input_uri(self):
        self.add_mock_s3_data({'walrus': {
            ATTEMPT_DIR + 'syslog': 
                CHILD_ERR_LINE +
                JAVA_STACK_TRACE +
                GARBAGE,
        }})
        assert_equal(self.runner._find_probable_cause_of_failure([1]),
                     {'lines': list(StringIO(JAVA_STACK_TRACE)),
                      's3_log_file_uri': 
                          BUCKET_URI + ATTEMPT_DIR + 'syslog',
                      'input_uri': None})

    def test_hadoop_streaming_error(self):
        # we should look only at step 2 since the errors in the other
        # steps are boring
        # 
        # we include input.gz just to test that we DON'T check for it
        self.add_mock_s3_data({'walrus': {
            LOG_DIR + 'steps/1/syslog':
                GARBAGE +
                HADOOP_ERR_LINE_PREFIX + BORING_HADOOP_ERROR + '\n',
            LOG_DIR + 'steps/2/syslog':
                GARBAGE +
                make_input_uri_line(BUCKET_URI + 'input.gz') +
                HADOOP_ERR_LINE_PREFIX + USEFUL_HADOOP_ERROR + '\n',
            LOG_DIR + 'steps/3/syslog':
                HADOOP_ERR_LINE_PREFIX + BORING_HADOOP_ERROR + '\n',
        }})

        assert_equal(self.runner._find_probable_cause_of_failure([1, 2, 3]),
                     {'lines': [USEFUL_HADOOP_ERROR + '\n'],
                      's3_log_file_uri':
                          BUCKET_URI + LOG_DIR + 'steps/2/syslog',
                      'input_uri': None})

    def test_later_task_attempt_steps_win(self):
        # should look at later steps first
        self.add_mock_s3_data({'walrus': {
            TASK_ATTEMPTS_DIR + 'attempt_201007271720_0001_r_000126_3/stderr':
                TRACEBACK_START + PY_EXCEPTION,
            TASK_ATTEMPTS_DIR + 'attempt_201007271720_0002_m_000004_0/syslog':
                CHILD_ERR_LINE + JAVA_STACK_TRACE,
        }})
        failure = self.runner._find_probable_cause_of_failure([1, 2])
        assert_equal(failure['s3_log_file_uri'],
                     BUCKET_URI + TASK_ATTEMPTS_DIR + 
                     'attempt_201007271720_0002_m_000004_0/syslog')

    def test_later_step_logs_win(self):
        self.add_mock_s3_data({'walrus': {
            LOG_DIR + 'steps/1/syslog':
                HADOOP_ERR_LINE_PREFIX + USEFUL_HADOOP_ERROR + '\n',
            LOG_DIR + 'steps/2/syslog':
                HADOOP_ERR_LINE_PREFIX + USEFUL_HADOOP_ERROR + '\n',
        }})
        failure = self.runner._find_probable_cause_of_failure([1, 2])
        assert_equal(failure['s3_log_file_uri'],
                     BUCKET_URI + LOG_DIR + 'steps/2/syslog')

    def test_reducer_beats_mapper(self):
        # should look at reducers over mappers
        self.add_mock_s3_data({'walrus': {
            TASK_ATTEMPTS_DIR + 'attempt_201007271720_0001_m_000126_3/stderr':
                TRACEBACK_START + PY_EXCEPTION,
            TASK_ATTEMPTS_DIR + 'attempt_201007271720_0001_r_000126_3/syslog':
                CHILD_ERR_LINE + JAVA_STACK_TRACE,
        }})
        failure = self.runner._find_probable_cause_of_failure([1])
        assert_equal(failure['s3_log_file_uri'],
                     BUCKET_URI + TASK_ATTEMPTS_DIR + 
                     'attempt_201007271720_0001_r_000126_3/syslog')

    def test_more_attempts_win(self):
        # look at fourth attempt before looking at first attempt
        self.add_mock_s3_data({'walrus': {
            TASK_ATTEMPTS_DIR + 'attempt_201007271720_0001_m_000126_0/stderr':
                TRACEBACK_START + PY_EXCEPTION,
            TASK_ATTEMPTS_DIR + 'attempt_201007271720_0001_m_000004_3/syslog':
                CHILD_ERR_LINE + JAVA_STACK_TRACE,
        }})
        failure = self.runner._find_probable_cause_of_failure([1])
        assert_equal(failure['s3_log_file_uri'],
                     BUCKET_URI + TASK_ATTEMPTS_DIR + 
                     'attempt_201007271720_0001_m_000004_3/syslog')

    def test_py_exception_beats_java_stack_trace(self):
        self.add_mock_s3_data({'walrus': {
            ATTEMPT_DIR + 'stderr': TRACEBACK_START + PY_EXCEPTION,
            ATTEMPT_DIR + 'syslog': CHILD_ERR_LINE + JAVA_STACK_TRACE,
        }})
        failure = self.runner._find_probable_cause_of_failure([1])
        assert_equal(failure['s3_log_file_uri'],
                     BUCKET_URI + ATTEMPT_DIR + 'stderr')

    def test_exception_beats_hadoop_error(self):
        self.add_mock_s3_data({'walrus': {
            TASK_ATTEMPTS_DIR + 'attempt_201007271720_0002_m_000126_0/stderr':
                TRACEBACK_START + PY_EXCEPTION,
            LOG_DIR + 'steps/1/syslog':
                HADOOP_ERR_LINE_PREFIX + USEFUL_HADOOP_ERROR + '\n',
        }})
        failure = self.runner._find_probable_cause_of_failure([1, 2])
        assert_equal(failure['s3_log_file_uri'],
                     BUCKET_URI + TASK_ATTEMPTS_DIR +
                     'attempt_201007271720_0002_m_000126_0/stderr')

    def test_step_filtering(self):
        # same as previous test, but step 2 is filtered out
        self.add_mock_s3_data({'walrus': {
            TASK_ATTEMPTS_DIR + 'attempt_201007271720_0002_m_000126_0/stderr':
                TRACEBACK_START + PY_EXCEPTION,
            LOG_DIR + 'steps/1/syslog':
                HADOOP_ERR_LINE_PREFIX + USEFUL_HADOOP_ERROR + '\n',
        }})
        failure = self.runner._find_probable_cause_of_failure([1])
        assert_equal(failure['s3_log_file_uri'],
                     BUCKET_URI + LOG_DIR + 'steps/1/syslog')


             
class TestLs(MockEMRAndS3TestCase):

    def test_s3_ls(self):
        runner = EMRJobRunner(s3_scratch_uri='s3://walrus/tmp',
                              conf_path=False)

        self.add_mock_s3_data({'walrus': {'one': '', 'two': '', 'three': ''}})

        assert_equal(set(runner._s3_ls('s3://walrus/')),
                     set(['s3://walrus/one',
                          's3://walrus/two',
                          's3://walrus/three',]))

        assert_equal(set(runner._s3_ls('s3://walrus/t')),
                     set(['s3://walrus/two',
                          's3://walrus/three',]))
        
        assert_equal(set(runner._s3_ls('s3://walrus/t/')),
                     set([]))

        # if we ask for a nonexistent bucket, we should get some sort
        # of exception (in practice, buckets with random names will
        # probably be owned by other people, and we'll get some sort
        # of permissions error)
        assert_raises(Exception, set, runner._s3_ls('s3://lolcat/'))
    
