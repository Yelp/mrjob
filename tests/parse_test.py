# -*- encoding: utf-8 -*-
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
from subprocess import Popen, PIPE
from testify import TestCase, assert_equal, assert_in, assert_not_equal, assert_raises

from mrjob.parse import *

class FindPythonTracebackTestCase(TestCase):

    def test_find_python_traceback(self):
        def run(*args):
            return Popen(args, stdout=PIPE, stderr=PIPE).communicate()

        # sanity-check normal operations
        ok_stdout, ok_stderr = run('python', '-c', "print sorted('321')")
        assert_equal(ok_stdout.rstrip(), "['1', '2', '3']")
        assert_equal(find_python_traceback(StringIO(ok_stderr)), None)

        # Oops, can't sort a number.
        stdout, stderr = run('python', '-c', "print sorted(321)")

        # We expect something like this:
        #
         # Traceback (most recent call last):
        #   File "<string>", line 1, in <module>
        # TypeError: 'int' object is not iterable
        assert_equal(stdout, '')
        # save the traceback for the next step
        tb = find_python_traceback(StringIO(stderr))
        assert_not_equal(tb, None)
        assert isinstance(tb, list)
        assert_equal(len(tb), 2) # The first line ("Traceback...") is skipped

        # make sure we can find the same traceback in noise
        verbose_stdout, verbose_stderr = run(
            'python', '-v', '-c', "print sorted(321)")
        assert_equal(verbose_stdout, '')
        assert_not_equal(verbose_stderr, stderr)
        verbose_tb = find_python_traceback(StringIO(verbose_stderr))
        assert_equal(verbose_tb, tb)

class FindMiscTestCase(TestCase):

    # we can't generate the output that the other find_*() methods look
    # for, so just search over some static data

    def test_empty(self):
        assert_equal(find_input_uri_for_mapper([]), None)
        assert_equal(find_hadoop_java_stack_trace([]), None)
        assert_equal(find_interesting_hadoop_streaming_error([]), None)

    def test_find_input_uri_for_mapper(self):
        LOG_LINES = [
            'garbage\n',
            "2010-07-27 17:54:54,344 INFO org.apache.hadoop.fs.s3native.NativeS3FileSystem (main): Opening 's3://yourbucket/logs/2010/07/23/log2-00077.gz' for reading\n",
            "2010-07-27 17:54:54,344 INFO org.apache.hadoop.fs.s3native.NativeS3FileSystem (main): Opening 's3://yourbucket/logs/2010/07/23/log2-00078.gz' for reading\n",
        ]
        assert_equal(find_input_uri_for_mapper(line for line in LOG_LINES),
                     's3://yourbucket/logs/2010/07/23/log2-00077.gz')

    def test_find_hadoop_java_stack_trace(self):
        LOG_LINES = [
            'java.lang.NameError: "Oak" was one character shorter\n',
            '2010-07-27 18:25:48,397 WARN org.apache.hadoop.mapred.TaskTracker (main): Error running child\n',
            'java.lang.OutOfMemoryError: Java heap space\n',
            '        at org.apache.hadoop.mapred.IFile$Reader.readNextBlock(IFile.java:270)\n',
            'BLARG\n',
            '        at org.apache.hadoop.mapred.IFile$Reader.next(IFile.java:332)\n',
        ]
        assert_equal(find_hadoop_java_stack_trace(line for line in LOG_LINES),
                     ['java.lang.OutOfMemoryError: Java heap space\n',
                      '        at org.apache.hadoop.mapred.IFile$Reader.readNextBlock(IFile.java:270)\n'])

    def test_find_interesting_hadoop_streaming_error(self):
        LOG_LINES = [
            '2010-07-27 19:53:22,451 ERROR org.apache.hadoop.streaming.StreamJob (main): Job not Successful!\n',
            '2010-07-27 19:53:35,451 ERROR org.apache.hadoop.streaming.StreamJob (main): Error launching job , Output path already exists : Output directory s3://yourbucket/logs/2010/07/23/ already exists and is not empty\n',
            '2010-07-27 19:53:52,451 ERROR org.apache.hadoop.streaming.StreamJob (main): Job not Successful!\n',
        ]

        assert_equal(
            find_interesting_hadoop_streaming_error(line for line in LOG_LINES),
            'Error launching job , Output path already exists : Output directory s3://yourbucket/logs/2010/07/23/ already exists and is not empty')
    
    def test_find_timeout_error(self):
        LOG_LINES = [
            'Task TASKID="task_201010202309_0001_m_000153" TASK_TYPE="MAP" TASK_STATUS="FAILED" FINISH_TIME="1287618918658" ERROR="Task attempt_201010202309_0001_m_000153_3 failed to report status for 602 seconds. Killing!"',
            'Task blahblah',
            'Bada bing!',
        ]
        
        assert_equal(find_timeout_error(LOG_LINES), 602)
        
        LOG_LINES = [
            'Job JOBID="job_201105252346_0001" LAUNCH_TIME="1306367213950" TOTAL_MAPS="2" TOTAL_REDUCES="1" ',
            'Task TASKID="task_201105252346_0001_m_000000" TASK_TYPE="MAP" START_TIME="1306367217455" SPLITS="/default-rack/localhost" ',
            'MapAttempt TASK_TYPE="MAP" TASKID="task_201105252346_0001_m_000000" TASK_ATTEMPT_ID="attempt_201105252346_0001_m_000000_0" START_TIME="1306367223172" HOSTNAME="/default-rack/ip-10-168-73-40.us-west-1.compute.internal" ',
            'Task TASKID="task_201105252346_0001_m_000000" TASK_TYPE="MAP" TASK_STATUS="FAILED" FINISH_TIME="1306367233379" ERROR="Task attempt_201105252346_0001_m_000000_3 failed to report status for 0 seconds. Killing!"',
        ]
        
        assert_equal(find_timeout_error(LOG_LINES), 0)

    def test_find_counters_0_18(self):
        counters = parse_hadoop_counters_from_line('Job JOBID="job_201106061823_0001" FINISH_TIME="1307384737542" JOB_STATUS="SUCCESS" FINISHED_MAPS="2" FINISHED_REDUCES="1" FAILED_MAPS="0" FAILED_REDUCES="0" COUNTERS="File Systems.S3N bytes read:3726,File Systems.Local bytes read:4164,File Systems.S3N bytes written:1663,File Systems.Local bytes written:8410,Job Counters .Launched reduce tasks:1,Job Counters .Rack-local map tasks:2,Job Counters .Launched map tasks:2,Map-Reduce Framework.Reduce input groups:154,Map-Reduce Framework.Combine output records:0,Map-Reduce Framework.Map input records:68,Map-Reduce Framework.Reduce output records:154,Map-Reduce Framework.Map output bytes:3446,Map-Reduce Framework.Map input bytes:2483,Map-Reduce Framework.Map output records:336,Map-Reduce Framework.Combine input records:0,Map-Reduce Framework.Reduce input records:336,profile.reducer step 0 estimated IO time: 0.00:1,profile.mapper step 0 estimated IO time: 0.00:2,profile.reducer step 0 estimated CPU time: 0.00:1,profile.mapper step ☃ estimated CPU time: 0.00:2"')

        assert_equal(counters['profile']['reducer step 0 estimated IO time: 0.00'], 1)
        assert_equal(counters['profile']['mapper step ☃ estimated CPU time: 0.00'], 2)

    def test_find_counters_0_20(self):
        counters = parse_hadoop_counters_from_line('Job JOBID="job_201106092314_0003" FINISH_TIME="1307662284564" JOB_STATUS="SUCCESS" FINISHED_MAPS="2" FINISHED_REDUCES="1" FAILED_MAPS="0" FAILED_REDUCES="0" COUNTERS="{(org\.apache\.hadoop\.mapred\.JobInProgress$Counter)(Job Counters )[(TOTAL_LAUNCHED_REDUCES)(Launched reduce tasks)(1)][(TOTAL_LAUNCHED_MAPS)(Launched map tasks)(2)][(DATA_LOCAL_MAPS)(Data-local map tasks)(2)]}{(FileSystemCounters)(FileSystemCounters)[(FILE_BYTES_READ)(FILE_BYTES_READ)(10547174)][(HDFS_BYTES_READ)(HDFS_BYTES_READ)(49661008)][(FILE_BYTES_WRITTEN)(FILE_BYTES_WRITTEN)(21773078)][(S3_BYTES_WRITTEN)(S3_BYTES_WRITTEN)(49526580)]}{(org\.apache\.hadoop\.mapred\.Task$Counter)(Map-Reduce Framework)[(REDUCE_INPUT_GROUPS)(Reduce input groups)(18843)][(COMBINE_OUTPUT_RECORDS)(Combine output records)(0)][(MAP_INPUT_RECORDS)(Map input records)(29884)][(REDUCE_SHUFFLE_BYTES)(Reduce shuffle bytes)(11225840)][(REDUCE_OUTPUT_RECORDS)(Reduce output records)(29884)][(SPILLED_RECORDS)(Spilled Records)(59768)][(MAP_OUTPUT_BYTES)(Map output bytes)(50285563)][(MAP_INPUT_BYTES)(Map input bytes)(49645726)][(MAP_OUTPUT_RECORDS)(Map output records)(29884)][(COMBINE_INPUT_RECORDS)(Combine input records)(0)][(REDUCE_INPUT_RECORDS)(Reduce input records)(29884)]}{(profile)(profile)[(reducer time \\(processing\\): 2\.51)(reducer time \\(processing\\): 2\.51)(1)][(mapper time \\(processing\\): 0\.50)(mapper time \\(processing\\): 0\.50)(1)][(mapper time \\(other\\): 3\.78)(mapper time \\(other\\): 3\.78)(1)][(mapper time \\(processing\\): 0\.46)(mapper time \\(processing\\): 0\.46)(1)][(reducer time \\(other\\): 6\.31)(reducer time \\(other\\): 6\.31)(1)][(mapper time \\(other\\): 3\.72)(mapper time \\(other\\): 3\.72)(1)]}" .')

        assert_in('reducer time (processing): 2.51', counters['profile'])

    def test_unescape(self):
        # cases covered by string_escape:
        assert_equal(counter_unescape(r'\n'), '\n')
        assert_equal(counter_unescape(r'\\'), '\\')
        # cases covered by manual unescape:
        assert_equal(counter_unescape(r'\.'), '.')
        assert_raises(ValueError, counter_unescape, '\\')

    def test_parsing_error(self):
        counter_string = 'Job FAILED_REDUCES="0" COUNTERS="{(testgroup)(testgroup)[(\\)(\\)(1)]}"'
        assert_raises(LogParsingException, parse_hadoop_counters_from_line, counter_string)

    def test_messy_error(self):
        counter_string = 'Job FAILED_REDUCES="0" COUNTERS="YOU JUST GOT PUNKD"'
        assert_raises(LogParsingException, parse_hadoop_counters_from_line, counter_string)


class ParseMRJobStderr(TestCase):

    def test_empty(self):
        assert_equal(parse_mr_job_stderr(StringIO()),
                     {'counters': {}, 'statuses': [], 'other': []})

    def test_parsing(self):
        INPUT = StringIO(
            'reporter:counter:Foo,Bar,2\n' +
            'reporter:status:Baz\n' +
            'reporter:status:Baz\n' +
            'reporter:counter:Foo,Bar,1\n' +
            'reporter:counter:Foo,Baz,1\n' +
            'reporter:counter:Quux Subsystem,Baz,42\n' +
            'Warning: deprecated metasyntactic variable: garply\n')

        assert_equal(
            parse_mr_job_stderr(INPUT),
            {'counters': {'Foo': {'Bar': 3, 'Baz': 1},
                          'Quux Subsystem': {'Baz': 42}},
             'statuses': ['Baz', 'Baz'],
             'other': ['Warning: deprecated metasyntactic variable: garply\n']
            })

    def test_update_counters(self):
        counters = {'Foo': {'Bar': 3, 'Baz': 1}}

        parse_mr_job_stderr(
            StringIO('reporter:counter:Foo,Baz,1\n'), counters=counters)

        assert_equal(counters, {'Foo': {'Bar': 3, 'Baz': 2}})

    def test_read_single_line(self):
        # LocalMRJobRunner runs parse_mr_job_stderr on one line at a time.
        assert_equal(parse_mr_job_stderr('reporter:counter:Foo,Bar,2\n'),
                     {'counters': {'Foo': {'Bar': 2}},
                      'statuses': [], 'other': []})

    def test_read_multiple_lines_from_buffer(self):
        assert_equal(parse_mr_job_stderr('reporter:counter:Foo,Bar,2\nwoot\n'),
                     {'counters': {'Foo': {'Bar': 2}},
                      'statuses': [], 'other': ['woot\n']})

    def test_negative_counters(self):
        # kind of poor practice to use negative counters, but Hadoop
        # Streaming supports it (negative numbers are integers too!)
        assert_equal(parse_mr_job_stderr(['reporter:counter:Foo,Bar,-2\n']),
                     {'counters': {'Foo': {'Bar': -2}},
                      'statuses': [], 'other': []})

    def test_garbled_counters(self):
        # we should be able to do something graceful with
        # garbled counters and status messages
        BAD_LINES = [
            'reporter:counter:Foo,Bar,Baz,1\n', # too many items
            'reporter:counter:Foo,1\n', # too few items
            'reporter:counter:Foo,Bar,a million\n', # not a number
            'reporter:counter:Foo,Bar,1.0\n', # not an int
            'reporter:crounter:Foo,Bar,1\n', # not a valid reporter
            'reporter,counter:Foo,Bar,1\n', # wrong format!
        ]

        assert_equal(parse_mr_job_stderr(BAD_LINES),
                     {'counters': {}, 'statuses': [], 'other': BAD_LINES})


class PortRangeListTestCase(TestCase):
    def test_port_range_list(self):
        assert_equal(parse_port_range_list('1234'), [1234]) 
        assert_equal(parse_port_range_list('123,456,789'), [123,456,789])
        assert_equal(parse_port_range_list('1234,5678'), [1234, 5678])
        assert_equal(parse_port_range_list('1234:1236'), [1234, 1235, 1236])
        assert_equal(parse_port_range_list('123:125,456'), [123,124,125,456])
        assert_equal(parse_port_range_list('123:125,456:458'), [123,124,125,456,457,458])
        assert_equal(parse_port_range_list('0123'), [123])

        assert_raises(ValueError, parse_port_range_list, 'Alexandria')
        assert_raises(ValueError, parse_port_range_list, 'Athens:Alexandria')

