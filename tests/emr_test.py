# Copyright 2009-2011 Yelp
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

"""Tests for EMRJobRunner"""

from __future__ import with_statement

import copy
import datetime
import getpass
import os
import shutil
from StringIO import StringIO
import tempfile
from testify import TestCase, assert_equal, assert_gt, assert_in, assert_not_in, assert_raises, setup, teardown

from mrjob.conf import dump_mrjob_conf
from mrjob.emr import EMRJobRunner, describe_all_job_flows, parse_s3_uri, DEFAULT_EC2_INSTANCE_TYPE
from mrjob.parse import JOB_NAME_RE
from tests.mockboto import MockS3Connection, MockEmrConnection, MockEmrObject, add_mock_s3_data, DEFAULT_MAX_DAYS_AGO, DEFAULT_MAX_JOB_FLOWS_RETURNED, to_iso8601
from tests.mr_two_step_job import MRTwoStepJob
from tests.quiet import logger_disabled

try:
    import boto
    from mrjob import botoemr
except ImportError:
    boto = None
    botoemr = None


class MockEMRAndS3TestCase(TestCase):

    @setup
    def make_mrjob_conf(self):
        _, self.mrjob_conf_path = tempfile.mkstemp(prefix='mrjob.conf.')
        with open(self.mrjob_conf_path, 'w') as f:
            dump_mrjob_conf({'runners': {'emr': {
                'check_emr_status_every': 0.01,
                's3_sync_wait_time': 0.01,
            }}}, f)

    @teardown
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
    def make_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

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

        mock_s3_fs_snapshot = copy.deepcopy(self.mock_s3_fs)

        with mr_job.make_runner() as runner:
            assert isinstance(runner, EMRJobRunner)

            # make sure that initializing the runner doesn't affect S3
            # (Issue #50)
            assert_equal(mock_s3_fs_snapshot, self.mock_s3_fs)

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
            name_match = JOB_NAME_RE.match(job_flow.name)
            assert_equal(name_match.group(1), 'mr_two_step_job')
            assert_equal(name_match.group(2), getpass.getuser())

            # make sure mrjob.tar.gz is created and uploaded as
            # a bootstrap file
            assert runner._mrjob_tar_gz_path
            mrjob_tar_gz_file_dicts = [
                file_dict for file_dict in runner._files
                if file_dict['path'] == runner._mrjob_tar_gz_path]

            assert_equal(len(mrjob_tar_gz_file_dicts), 1)

            mrjob_tar_gz_file_dict = mrjob_tar_gz_file_dicts[0]
            assert mrjob_tar_gz_file_dict['name']
            assert_equal(mrjob_tar_gz_file_dict.get('bootstrap'), 'file')

            # shouldn't be in PYTHONPATH (we dump it directly in site-packages)
            pythonpath = runner._get_cmdenv().get('PYTHONPATH') or ''
            assert_not_in(mrjob_tar_gz_file_dict['name'],
                          pythonpath.split(':'))

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

            with logger_disabled('mrjob.emr'):
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


class S3ScratchURITestCase(MockEMRAndS3TestCase):

    def test_pick_scratch_uri(self):
        self.add_mock_s3_data({'mrjob-walrus': {}, 'zebra': {}})
        runner = EMRJobRunner(conf_path=False)

        assert_equal(runner._opts['s3_scratch_uri'],
                     's3://mrjob-walrus/tmp/')

    def test_create_scratch_uri(self):
        # "walrus" bucket will be ignored; it doesn't start with "mrjob-"
        self.add_mock_s3_data({'walrus': {}, 'zebra': {}})

        runner = EMRJobRunner(conf_path=False, s3_sync_wait_time=0.01)

        # bucket name should be mrjob- plus 16 random hex digits
        s3_scratch_uri = runner._opts['s3_scratch_uri']
        assert_equal(s3_scratch_uri[:11], 's3://mrjob-')
        assert_equal(s3_scratch_uri[27:], '/tmp/')

        # bucket shouldn't actually exist yet
        scratch_bucket, _ = parse_s3_uri(s3_scratch_uri)
        assert_not_in(scratch_bucket, self.mock_s3_fs.keys())

        # need to do something to ensure that the bucket actually gets
        # created. let's launch a (mock) job flow
        job_flow_id = runner.make_persistent_job_flow()
        assert_in(scratch_bucket, self.mock_s3_fs.keys())
        runner.make_emr_conn().terminate_jobflow(job_flow_id)

        # once our scratch bucket is created, we should re-use it
        runner2 = EMRJobRunner(conf_path=False)
        assert_equal(runner2._opts['s3_scratch_uri'], s3_scratch_uri)
        s3_scratch_uri = runner._opts['s3_scratch_uri']


class BootstrapFilesTestCase(MockEMRAndS3TestCase):

    def test_bootstrap_files_only_get_uploaded_once(self):
        # just a regression test for Issue #8

        # use self.mrjob_conf_path because it's easier than making a new file
        bootstrap_file = self.mrjob_conf_path

        runner = EMRJobRunner(conf_path=False,
                              bootstrap_files=[bootstrap_file])

        matching_file_dicts = [fd for fd in runner._files
                               if fd['path'] == bootstrap_file]
        assert_equal(len(matching_file_dicts), 1)


class DescribeAllJobFlowsTestCase(MockEMRAndS3TestCase):

    def test_can_get_all_job_flows(self):
        now = datetime.datetime.utcnow()

        NUM_JOB_FLOWS = 2222
        assert_gt(NUM_JOB_FLOWS, DEFAULT_MAX_JOB_FLOWS_RETURNED)

        for i in range(NUM_JOB_FLOWS):
            job_flow_id = 'j-%04d' % i
            self.mock_emr_job_flows[job_flow_id] = MockEmrObject(
                creationdatetime=to_iso8601(
                    now - datetime.timedelta(minutes=i)),
                jobflowid=job_flow_id)

        emr_conn = EMRJobRunner().make_emr_conn()

        # ordinary describe_jobflows() hits the limit on number of job flows
        some_job_flows = emr_conn.describe_jobflows()
        assert_equal(len(some_job_flows), DEFAULT_MAX_JOB_FLOWS_RETURNED)

        all_job_flows = describe_all_job_flows(emr_conn)
        assert_equal(len(all_job_flows), NUM_JOB_FLOWS)
        assert_equal(sorted(jf.jobflowid for jf in all_job_flows),
                     [('j-%04d' % i) for i in range(NUM_JOB_FLOWS)])


class EC2InstanceTypeTestCase(MockEMRAndS3TestCase):

    def _test_instance_types(self, kwargs, expected_master, expected_slave):
        runner = EMRJobRunner(conf_path=self.mrjob_conf_path, **kwargs)

        job_flow_id = runner.make_persistent_job_flow()
        job_flow = runner.make_emr_conn().describe_jobflow(job_flow_id)

        assert_equal(
            (expected_master, expected_slave),
            (job_flow.masterinstancetype, job_flow.slaveinstancetype))

    def test_defaults(self):
        self._test_instance_types(
            {}, DEFAULT_EC2_INSTANCE_TYPE, DEFAULT_EC2_INSTANCE_TYPE)

        self._test_instance_types(
            {'num_ec2_instances': 2},
            DEFAULT_EC2_INSTANCE_TYPE, DEFAULT_EC2_INSTANCE_TYPE)

    def test_single_instance(self):
        self._test_instance_types(
            {'ec2_instance_type': 'c1.xlarge'},
            'c1.xlarge', 'c1.xlarge')

    def test_multiple_instances(self):
        self._test_instance_types(
            {'ec2_instance_type': 'c1.xlarge', 'num_ec2_instances': 2},
            DEFAULT_EC2_INSTANCE_TYPE, 'c1.xlarge')

    def test_explicit_master_and_slave_instance_types(self):
        self._test_instance_types(
            {'ec2_master_instance_type': 'm1.large'},
            'm1.large', DEFAULT_EC2_INSTANCE_TYPE)
        self._test_instance_types(
            {'ec2_slave_instance_type': 'm2.xlarge'},
            DEFAULT_EC2_INSTANCE_TYPE, 'm2.xlarge')
        self._test_instance_types(
            {'ec2_master_instance_type': 'm1.large',
             'ec2_slave_instance_type': 'm2.xlarge'},
            'm1.large', 'm2.xlarge')

    def test_ec2_instance_type_takes_precedence(self):
        self._test_instance_types(
            {'ec2_instance_type': 'c1.xlarge',
             'ec2_master_instance_type': 'm1.large',
             'ec2_slave_instance_type': 'm2.xlarge'},
            'c1.xlarge', 'c1.xlarge')
        # when there are multiple instances, ec2_instance_type only
        # sets slave instance type
        self._test_instance_types(
            {'ec2_instance_type': 'c1.xlarge',
             'ec2_master_instance_type': 'm1.large',
             'num_ec2_instances': 2,
             'ec2_slave_instance_type': 'm2.xlarge'},
            'm1.large', 'c1.xlarge')
        
        
        
        
                                 


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

ATTEMPT_0_DIR = TASK_ATTEMPTS_DIR + 'attempt_201007271720_0001_m_000126_0/'
ATTEMPT_1_DIR = TASK_ATTEMPTS_DIR + 'attempt_201007271720_0001_m_000126_0/'

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
            ATTEMPT_0_DIR + 'stderr':
                GARBAGE + TRACEBACK_START + PY_EXCEPTION + GARBAGE,
            ATTEMPT_0_DIR + 'syslog':
                make_input_uri_line(BUCKET_URI + 'input.gz'),
        }})
        assert_equal(self.runner._find_probable_cause_of_failure([1]),
                     {'lines': list(StringIO(PY_EXCEPTION)),
                      's3_log_file_uri':
                          BUCKET_URI + ATTEMPT_0_DIR + 'stderr',
                      'input_uri': BUCKET_URI + 'input.gz'})

    def test_python_exception_without_input_uri(self):
        self.add_mock_s3_data({'walrus': {
            ATTEMPT_0_DIR + 'stderr': (
                GARBAGE + TRACEBACK_START + PY_EXCEPTION + GARBAGE),
        }})
        assert_equal(self.runner._find_probable_cause_of_failure([1]),
                     {'lines': list(StringIO(PY_EXCEPTION)),
                      's3_log_file_uri':
                          BUCKET_URI + ATTEMPT_0_DIR + 'stderr',
                      'input_uri': None})

    def test_java_exception(self):
        self.add_mock_s3_data({'walrus': {
            ATTEMPT_0_DIR + 'stderr': GARBAGE + GARBAGE,
            ATTEMPT_0_DIR + 'syslog':
                make_input_uri_line(BUCKET_URI + 'input.gz') +
                GARBAGE +
                CHILD_ERR_LINE +
                JAVA_STACK_TRACE +
                GARBAGE,
        }})
        assert_equal(self.runner._find_probable_cause_of_failure([1]),
                     {'lines': list(StringIO(JAVA_STACK_TRACE)),
                      's3_log_file_uri':
                          BUCKET_URI + ATTEMPT_0_DIR + 'syslog',
                      'input_uri': BUCKET_URI + 'input.gz'})

    def test_java_exception_without_input_uri(self):
        self.add_mock_s3_data({'walrus': {
            ATTEMPT_0_DIR + 'syslog':
                CHILD_ERR_LINE +
                JAVA_STACK_TRACE +
                GARBAGE,
        }})
        assert_equal(self.runner._find_probable_cause_of_failure([1]),
                     {'lines': list(StringIO(JAVA_STACK_TRACE)),
                      's3_log_file_uri':
                          BUCKET_URI + ATTEMPT_0_DIR + 'syslog',
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
            ATTEMPT_0_DIR + 'stderr': TRACEBACK_START + PY_EXCEPTION,
            ATTEMPT_0_DIR + 'syslog': CHILD_ERR_LINE + JAVA_STACK_TRACE,
        }})
        failure = self.runner._find_probable_cause_of_failure([1])
        assert_equal(failure['s3_log_file_uri'],
                     BUCKET_URI + ATTEMPT_0_DIR + 'stderr')

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

    def test_ignore_errors_from_steps_that_later_succeeded(self):
        # This tests the fix for Issue #31
        self.add_mock_s3_data({'walrus': {
            ATTEMPT_0_DIR + 'stderr':
                GARBAGE + TRACEBACK_START + PY_EXCEPTION + GARBAGE,
            ATTEMPT_0_DIR + 'syslog':
                make_input_uri_line(BUCKET_URI + 'input.gz'),
            ATTEMPT_1_DIR + 'stderr': '',
            ATTEMPT_1_DIR + 'syslog':
                make_input_uri_line(BUCKET_URI + 'input.gz'),
        }})
        assert_equal(self.runner._find_probable_cause_of_failure([1]), None)


class TestEMRandS3Endpoints(MockEMRAndS3TestCase):

    def test_no_region(self):
        runner = EMRJobRunner(conf_path=False)
        assert_equal(runner.make_emr_conn().endpoint,
                     'elasticmapreduce.amazonaws.com')
        assert_equal(runner.make_s3_conn().endpoint,
                     's3.amazonaws.com')
        assert_equal(runner._aws_region, '')

    def test_none_region(self):
        # blank region should be treated the same as no region
        runner = EMRJobRunner(conf_path=False, aws_region=None)
        assert_equal(runner.make_emr_conn().endpoint,
                     'elasticmapreduce.amazonaws.com')
        assert_equal(runner.make_s3_conn().endpoint,
                     's3.amazonaws.com')
        assert_equal(runner._aws_region, '')

    def test_blank_region(self):
        # blank region should be treated the same as no region
        runner = EMRJobRunner(conf_path=False, aws_region='')
        assert_equal(runner.make_emr_conn().endpoint,
                     'elasticmapreduce.amazonaws.com')
        assert_equal(runner.make_s3_conn().endpoint,
                     's3.amazonaws.com')
        assert_equal(runner._aws_region, '')

    def test_eu(self):
        runner = EMRJobRunner(conf_path=False, aws_region='EU')
        assert_equal(runner.make_emr_conn().endpoint,
                     'eu-west-1.elasticmapreduce.amazonaws.com')
        assert_equal(runner.make_s3_conn().endpoint,
                     's3-eu-west-1.amazonaws.com')

    def test_us_east_1(self):
        runner = EMRJobRunner(conf_path=False, aws_region='us-east-1')
        assert_equal(runner.make_emr_conn().endpoint,
                     'us-east-1.elasticmapreduce.amazonaws.com')
        assert_equal(runner.make_s3_conn().endpoint,
                     's3.amazonaws.com')

    def test_us_west_1(self):
        runner = EMRJobRunner(conf_path=False, aws_region='us-west-1')
        assert_equal(runner.make_emr_conn().endpoint,
                     'us-west-1.elasticmapreduce.amazonaws.com')
        assert_equal(runner.make_s3_conn().endpoint,
                     's3-us-west-1.amazonaws.com')

    def test_ap_southeast_1(self):
        runner = EMRJobRunner(conf_path=False, aws_region='ap-southeast-1')
        assert_equal(runner.make_s3_conn().endpoint,
                     's3-ap-southeast-1.amazonaws.com')
        assert_raises(Exception, runner.make_emr_conn)

    def test_bad_region(self):
        # should fail in the constructor because the constructor connects to S3
        assert_raises(Exception, EMRJobRunner,
                      conf_path=False, aws_region='the-moooooooon-1')

    def test_case_sensitive(self):
        assert_raises(Exception, EMRJobRunner,
                      conf_path=False, aws_region='eu')
        assert_raises(Exception, EMRJobRunner,
                      conf_path=False, aws_region='US-WEST-1')

    def test_explicit_endpoints(self):
        runner = EMRJobRunner(conf_path=False, aws_region='EU',
                              s3_endpoint='s3-proxy', emr_endpoint='emr-proxy')
        assert_equal(runner.make_emr_conn().endpoint, 'emr-proxy')
        assert_equal(runner.make_s3_conn().endpoint, 's3-proxy')


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

