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
from __future__ import with_statement

from StringIO import StringIO
from subprocess import PIPE
from subprocess import Popen
from testify import TestCase
from testify import assert_equal
from testify import assert_in
from testify import assert_not_equal
from testify import assert_raises
import logging

from mrjob.parse import counter_unescape
from mrjob.parse import find_hadoop_java_stack_trace
from mrjob.parse import find_input_uri_for_mapper
from mrjob.parse import find_interesting_hadoop_streaming_error
from mrjob.parse import find_job_log_multiline_error
from mrjob.parse import find_python_traceback
from mrjob.parse import find_timeout_error
from mrjob.parse import is_s3_uri
from mrjob.parse import is_uri
from mrjob.parse import parse_hadoop_counters_from_line
from mrjob.parse import parse_mr_job_stderr
from mrjob.parse import parse_port_range_list
from mrjob.parse import parse_s3_uri
from mrjob.parse import urlparse
from mrjob.util import log_to_stream
from tests.quiet import no_handlers_for_logger


class FindPythonTracebackTestCase(TestCase):

    EXAMPLE_TRACEBACK = """Traceback (most recent call last):
  File "mr_collect_per_search_info_remote.py", line 8, in <module>
    from batch.stat_loader_remote.protocols import MySQLLoadProtocol
  File "/mnt/var/lib/hadoop/mapred/taskTracker/jobcache/job_201108022217_0001/attempt_201108022217_0001_m_000001_0/work/yelp-src-tree. tar.gz/batch/stat_loader_remote/protocols.py", line 4, in <module>
ImportError: No module named mr3po.mysqldump
Traceback (most recent call last):
  File "wrapper.py", line 16, in <module>
    check_call(sys.argv[1:])
  File "/usr/lib/python2.5/subprocess.py", line 462, in check_call
    raise CalledProcessError(retcode, cmd)
subprocess.CalledProcessError: Command '['python', 'mr_collect_per_search_info_remote.py', '--step-num=0', '--mapper']' returned non-  zero exit status 1
"""

    EXAMPLE_STDERR_TRACEBACK_1 = """mr_profile_test.py:27: Warning: 'with' will become a reserved keyword in Python 2.6
  File "mr_profile_test.py", line 27
    with open('/mnt/var/log/hadoop/profile/bloop', 'w') as f:
            ^
SyntaxError: invalid syntax
Traceback (most recent call last):
  File "wrapper.py", line 16, in <module>
    check_call(sys.argv[1:])
  File "/usr/lib/python2.5/subprocess.py", line 462, in check_call
    raise CalledProcessError(retcode, cmd)
subprocess.CalledProcessError: Command '['python', 'mr_profile_test.py', '--step-num=0', '--reducer', '--input-protocol', 'raw_value', '--output-protocol', 'json', '--protocol', 'json']' returned non-zero exit status 1
"""

    EXAMPLE_STDERR_TRACEBACK_2 = """tools/csv-to-myisam.c:18:19: error: mysql.h: No such file or directory
make: *** [tools/csv-to-myisam] Error 1
Traceback (most recent call last):
  File "wrapper.py", line 11, in <module>
    check_call('cd yelp-src-tree.tar.gz; ln -sf $(readlink -f config/emr/level.py) config/level.py; make -f Makefile.emr', shell=True, stdout=open('/dev/null', 'w'))
  File "/usr/lib/python2.5/subprocess.py", line 462, in check_call
    raise CalledProcessError(retcode, cmd)
subprocess.CalledProcessError: Command 'cd yelp-src-tree.tar.gz; ln -sf $(readlink -f config/emr/level.py) config/level.py; make -f    Makefile.emr' returned non-zero exit status 2
"""

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
        assert_equal(len(tb), 3)  # The first line ("Traceback...") is not skipped

        # make sure we can find the same traceback in noise
        verbose_stdout, verbose_stderr = run(
            'python', '-v', '-c', "print sorted(321)")
        assert_equal(verbose_stdout, '')
        assert_not_equal(verbose_stderr, stderr)
        verbose_tb = find_python_traceback(StringIO(verbose_stderr))
        assert_equal(verbose_tb, tb)

    def test_find_multiple_python_tracebacks(self):
        total_traceback = self.EXAMPLE_TRACEBACK + 'junk\n'
        tb = find_python_traceback(StringIO(total_traceback))
        assert_equal(''.join(tb), self.EXAMPLE_TRACEBACK)

    def test_find_python_traceback_with_more_stderr(self):
        total_traceback = self.EXAMPLE_STDERR_TRACEBACK_1 + 'junk\n'
        tb = find_python_traceback(StringIO(total_traceback))
        assert_equal(''.join(tb), self.EXAMPLE_STDERR_TRACEBACK_1)

    def test_find_python_traceback_with_more_stderr_2(self):
        total_traceback = self.EXAMPLE_STDERR_TRACEBACK_2 + 'junk\n'
        tb = find_python_traceback(StringIO(total_traceback))
        assert_equal(''.join(tb), self.EXAMPLE_STDERR_TRACEBACK_2)


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

    def test_find_timeout_error_1(self):
        LOG_LINES = [
            'Task TASKID="task_201010202309_0001_m_000153" TASK_TYPE="MAP" TASK_STATUS="FAILED" FINISH_TIME="1287618918658" ERROR="Task attempt_201010202309_0001_m_000153_3 failed to report status for 602 seconds. Killing!"',
            'Task TASKID="task_201010202309_0001_m_000153" TASK_TYPE="MAP" TASK_STATUS="FAILED" FINISH_TIME="1287618918658" ERROR="Task attempt_201010202309_0001_m_000153_3 failed to report status for 602 seconds. Killing!"',
            'Task blahblah',
            'Bada bing!',
        ]
        LOG_LINES_2 = [
            'Not a match',
        ]

        assert_equal(find_timeout_error(LOG_LINES), 602)
        assert_equal(find_timeout_error(LOG_LINES_2), None)

    def test_find_timeout_error_2(self):
        LOG_LINES = [
            'Job JOBID="job_201105252346_0001" LAUNCH_TIME="1306367213950" TOTAL_MAPS="2" TOTAL_REDUCES="1" ',
            'Task TASKID="task_201105252346_0001_m_000000" TASK_TYPE="MAP" START_TIME="1306367217455" SPLITS="/default-rack/localhost" ',
            'MapAttempt TASK_TYPE="MAP" TASKID="task_201105252346_0001_m_000000" TASK_ATTEMPT_ID="attempt_201105252346_0001_m_000000_0" START_TIME="1306367223172" HOSTNAME="/default-rack/ip-10-168-73-40.us-west-1.compute.internal" ',
            'Task TASKID="task_201105252346_0001_m_000000" TASK_TYPE="MAP" TASK_STATUS="FAILED" FINISH_TIME="1306367233379" ERROR="Task attempt_201105252346_0001_m_000000_3 failed to report status for 0 seconds. Killing!"',
        ]

        assert_equal(find_timeout_error(LOG_LINES), 0)

    def test_find_timeout_error_3(self):
        LOG_LINES = [
           'MapAttempt TASK_TYPE="MAP" TASKID="task_201107201804_0001_m_000160" TASK_ATTEMPT_ID="attempt_201107201804_0001_m_000160_0" TASK_STATUS="FAILED" FINISH_TIME="1311188233290" HOSTNAME="/default-rack/ip-10-160-243-66.us-west-1.compute.internal" ERROR="Task attempt_201107201804_0001_m_000160_0 failed to report status for 1201 seconds. Killing!"  '
        ]

        assert_equal(find_timeout_error(LOG_LINES), 1201)

    def test_find_multiline_job_log_error(self):
        LOG_LINES = [
            'junk',
            'MapAttempt TASK_TYPE="MAP" TASKID="task_201106280040_0001_m_000218" TASK_ATTEMPT_ID="attempt_201106280040_0001_m_000218_5" TASK_STATUS="FAILED" FINISH_TIME="1309246900665" HOSTNAME="/default-rack/ip-10-166-239-133.us-west-1.compute.internal" ERROR="Error initializing attempt_201106280040_0001_m_000218_5:',
            'java.io.IOException: Cannot run program "bash": java.io.IOException: error=12, Cannot allocate memory',
            '    ... 10 more',
            '"',
            'junk'
        ]
        SHOULD_EQUAL = [
            'Error initializing attempt_201106280040_0001_m_000218_5:',
            'java.io.IOException: Cannot run program "bash": java.io.IOException: error=12, Cannot allocate memory',
            '    ... 10 more',
        ]
        assert_equal(find_job_log_multiline_error(line for line in LOG_LINES), SHOULD_EQUAL)

    def test_find_counters_0_18(self):
        counters, step_num = parse_hadoop_counters_from_line('Job JOBID="job_201106061823_0001" FINISH_TIME="1307384737542" JOB_STATUS="SUCCESS" FINISHED_MAPS="2" FINISHED_REDUCES="1" FAILED_MAPS="0" FAILED_REDUCES="0" COUNTERS="File Systems.S3N bytes read:3726,File Systems.Local bytes read:4164,File Systems.S3N bytes written:1663,File Systems.Local bytes written:8410,Job Counters .Launched reduce tasks:1,Job Counters .Rack-local map tasks:2,Job Counters .Launched map tasks:2,Map-Reduce Framework.Reduce input groups:154,Map-Reduce Framework.Combine output records:0,Map-Reduce Framework.Map input records:68,Map-Reduce Framework.Reduce output records:154,Map-Reduce Framework.Map output bytes:3446,Map-Reduce Framework.Map input bytes:2483,Map-Reduce Framework.Map output records:336,Map-Reduce Framework.Combine input records:0,Map-Reduce Framework.Reduce input records:336,profile.reducer step 0 estimated IO time: 0.00:1,profile.mapper step 0 estimated IO time: 0.00:2,profile.reducer step 0 estimated CPU time: 0.00:1,profile.mapper step ☃ estimated CPU time: 0.00:2"')

        assert_equal(counters['profile']['reducer step 0 estimated IO time: 0.00'], 1)
        assert_equal(counters['profile']['mapper step ☃ estimated CPU time: 0.00'], 2)
        assert_equal(step_num, 1)

    def test_find_counters_0_20(self):
        counters, step_num = parse_hadoop_counters_from_line(r'Job JOBID="job_201106092314_0003" FINISH_TIME="1307662284564" JOB_STATUS="SUCCESS" FINISHED_MAPS="2" FINISHED_REDUCES="1" FAILED_MAPS="0" FAILED_REDUCES="0" COUNTERS="{(org\.apache\.hadoop\.mapred\.JobInProgress$Counter)(Job Counters )[(TOTAL_LAUNCHED_REDUCES)(Launched reduce tasks)(1)][(TOTAL_LAUNCHED_MAPS)(Launched map tasks)(2)][(DATA_LOCAL_MAPS)(Data-local map tasks)(2)]}{(FileSystemCounters)(FileSystemCounters)[(FILE_BYTES_READ)(FILE_BYTES_READ)(10547174)][(HDFS_BYTES_READ)(HDFS_BYTES_READ)(49661008)][(FILE_BYTES_WRITTEN)(FILE_BYTES_WRITTEN)(21773078)][(S3_BYTES_WRITTEN)(S3_BYTES_WRITTEN)(49526580)]}{(org\.apache\.hadoop\.mapred\.Task$Counter)(Map-Reduce Framework)[(REDUCE_INPUT_GROUPS)(Reduce input groups)(18843)][(COMBINE_OUTPUT_RECORDS)(Combine output records)(0)][(MAP_INPUT_RECORDS)(Map input records)(29884)][(REDUCE_SHUFFLE_BYTES)(Reduce shuffle bytes)(11225840)][(REDUCE_OUTPUT_RECORDS)(Reduce output records)(29884)][(SPILLED_RECORDS)(Spilled Records)(59768)][(MAP_OUTPUT_BYTES)(Map output bytes)(50285563)][(MAP_INPUT_BYTES)(Map input bytes)(49645726)][(MAP_OUTPUT_RECORDS)(Map output records)(29884)][(COMBINE_INPUT_RECORDS)(Combine input records)(0)][(REDUCE_INPUT_RECORDS)(Reduce input records)(29884)]}{(profile)(profile)[(reducer time \\(processing\\): 2\.51)(reducer time \\(processing\\): 2\.51)(1)][(mapper time \\(processing\\): 0\.50)(mapper time \\(processing\\): 0\.50)(1)][(mapper time \\(other\\): 3\.78)(mapper time \\(other\\): 3\.78)(1)][(mapper time \\(processing\\): 0\.46)(mapper time \\(processing\\): 0\.46)(1)][(reducer time \\(other\\): 6\.31)(reducer time \\(other\\): 6\.31)(1)][(mapper time \\(other\\): 3\.72)(mapper time \\(other\\): 3\.72)(1)]}" .')

        assert_in('reducer time (processing): 2.51', counters['profile'])
        assert_equal(step_num, 3)

    def test_find_weird_counters_0_20(self):
        counters, step_num = parse_hadoop_counters_from_line(r'Job JOBID="job_201106132124_0001" FINISH_TIME="1308000435810" JOB_STATUS="SUCCESS" FINISHED_MAPS="2" FINISHED_REDUCES="1" FAILED_MAPS="0" FAILED_REDUCES="0" COUNTERS="{(org\.apache\.hadoop\.mapred\.JobInProgress$Counter)(Job Counters )[(TOTAL_LAUNCHED_REDUCES)(Launched reduce tasks)(1)][(RACK_LOCAL_MAPS)(Rack-local map tasks)(2)][(TOTAL_LAUNCHED_MAPS)(Launched map tasks)(2)]}{(FileSystemCounters)(FileSystemCounters)[(FILE_BYTES_READ)(FILE_BYTES_READ)(1494)][(S3_BYTES_READ)(S3_BYTES_READ)(3726)][(FILE_BYTES_WRITTEN)(FILE_BYTES_WRITTEN)(3459)][(S3_BYTES_WRITTEN)(S3_BYTES_WRITTEN)(1663)]}{(weird counters)(weird counters)[(\\[\\])(\\[\\])(68)][(\\\\)(\\\\)(68)][(\\{\\})(\\{\\})(68)][(\\(\\))(\\(\\))(68)][(\.)(\.)(68)]}{(org\.apache\.hadoop\.mapred\.Task$Counter)(Map-Reduce Framework)[(REDUCE_INPUT_GROUPS)(Reduce input groups)(154)][(COMBINE_OUTPUT_RECORDS)(Combine output records)(0)][(MAP_INPUT_RECORDS)(Map input records)(68)][(REDUCE_SHUFFLE_BYTES)(Reduce shuffle bytes)(1901)][(REDUCE_OUTPUT_RECORDS)(Reduce output records)(154)][(SPILLED_RECORDS)(Spilled Records)(672)][(MAP_OUTPUT_BYTES)(Map output bytes)(3446)][(MAP_INPUT_BYTES)(Map input bytes)(2483)][(MAP_OUTPUT_RECORDS)(Map output records)(336)][(COMBINE_INPUT_RECORDS)(Combine input records)(0)][(REDUCE_INPUT_RECORDS)(Reduce input records)(336)]}" .')

        assert_in('{}', counters['weird counters'])
        assert_in('()', counters['weird counters'])
        assert_in('.', counters['weird counters'])
        assert_in('[]', counters['weird counters'])
        assert_in('\\', counters['weird counters'])
        assert_equal(step_num, 1)

    def test_unescape(self):
        # cases covered by string_escape:
        assert_equal(counter_unescape(r'\n'), '\n')
        assert_equal(counter_unescape(r'\\'), '\\')
        # cases covered by manual unescape:
        assert_equal(counter_unescape(r'\.'), '.')
        assert_raises(ValueError, counter_unescape, '\\')

    def test_messy_error(self):
        counter_string = 'Job JOBID="_001" FAILED_REDUCES="0" COUNTERS="THIS IS NOT ACTUALLY A COUNTER"'
        with no_handlers_for_logger(''):
            stderr = StringIO()
            log_to_stream('mrjob.parse', stderr, level=logging.WARN)
            assert_equal((None, None), parse_hadoop_counters_from_line(counter_string))
            assert_in('Cannot parse Hadoop counter line', stderr.getvalue())

    def test_freaky_counter_names(self):
        freaky_name = r'\\\\\{\}\(\)\[\]\.\\\\'
        counter_string = r'Job JOBID="_001" FAILED_REDUCES="0" COUNTERS="{(%s)(%s)[(a)(a)(1)]}"' % (freaky_name, freaky_name)
        assert_in('\\{}()[].\\', parse_hadoop_counters_from_line(counter_string)[0])

    def test_counters_fuzz(self):
        # test some strings that should break badly formulated parsing regexps
        freakquences = [
            ('\\[\\]\\(\\}\\[\\{\\\\\\\\\\[\\]\\(', '[](}[{\\[]('),
            ('\\)\\}\\\\\\\\\\[\\[\\)\\{\\{\\}\\]', ')}\\[[){{}]'),
            ('\\(\\{\\(\\[\\(\\]\\\\\\\\\\(\\\\\\\\\\\\\\\\', '({([(]\\(\\\\'),
            ('\\)\\{\\[\\)\\)\\(\\}\\(\\\\\\\\\\\\\\\\', '){[))(}(\\\\'),
            ('\\}\\(\\{\\)\\]\\]\\(\\]\\[\\\\\\\\', '}({)]](][\\'),
            ('\\[\\{\\\\\\\\\\)\\\\\\\\\\{\\{\\]\\]\\(', '[{\\)\\{{]]('),
            ('\\\\\\\\\\(\\(\\)\\\\\\\\\\\\\\\\\\\\\\\\\\[\\{\\]', '\\(()\\\\\\[{]'),
            ('\\]\\(\\[\\)\\{\\(\\)\\)\\{\\]', ']([){()){]'),
            ('\\(\\[\\{\\[\\[\\(\\{\\}\\(\\{', '([{[[({}({'),
            ('\\(\\{\\(\\{\\[\\{\\(\\{\\}\\}', '({({[{({}}')]
        for in_str, out_str in freakquences:
            counter_string = r'Job JOBID="_001" FAILED_REDUCES="0" COUNTERS="{(%s)(%s)[(a)(a)(1)]}"' % (in_str, in_str)
            assert_in(out_str,
                      parse_hadoop_counters_from_line(counter_string)[0])


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
            'reporter:counter:Foo,Bar,Baz,1\n',  # too many items
            'reporter:counter:Foo,1\n',  # too few items
            'reporter:counter:Foo,Bar,a million\n',  # not a number
            'reporter:counter:Foo,Bar,1.0\n',  # not an int
            'reporter:crounter:Foo,Bar,1\n',  # not a valid reporter
            'reporter,counter:Foo,Bar,1\n',  # wrong format!
        ]

        assert_equal(parse_mr_job_stderr(BAD_LINES),
                     {'counters': {}, 'statuses': [], 'other': BAD_LINES})


class PortRangeListTestCase(TestCase):
    def test_port_range_list(self):
        assert_equal(parse_port_range_list('1234'), [1234])
        assert_equal(parse_port_range_list('123,456,789'), [123, 456, 789])
        assert_equal(parse_port_range_list('1234,5678'), [1234, 5678])
        assert_equal(parse_port_range_list('1234:1236'), [1234, 1235, 1236])
        assert_equal(parse_port_range_list('123:125,456'),
                     [123, 124, 125, 456])
        assert_equal(parse_port_range_list('123:125,456:458'),
                     [123, 124, 125, 456, 457, 458])
        assert_equal(parse_port_range_list('0123'), [123])

        assert_raises(ValueError, parse_port_range_list, 'Alexandria')
        assert_raises(ValueError, parse_port_range_list, 'Athens:Alexandria')


class URITestCase(TestCase):
    def test_uri_parsing(self):
        assert_equal(is_uri('notauri!'), False)
        assert_equal(is_uri('they://did/the/monster/mash'), True)
        assert_equal(is_s3_uri('s3://a/uri'), True)
        assert_equal(is_s3_uri('s3n://a/uri'), True)
        assert_equal(is_s3_uri('hdfs://a/uri'), False)
        assert_equal(parse_s3_uri('s3://bucket/loc'), ('bucket', 'loc'))

    def test_urlparse(self):
        assert_equal(urlparse('http://www.yelp.com/lil_brudder'),
                     ('http', 'www.yelp.com', '/lil_brudder', '', '', ''))
        assert_equal(urlparse('cant://touch/this'),
                     ('cant', 'touch', '/this', '', '', ''))
        assert_equal(urlparse('s3://bucket/path'),
                     ('s3', 'bucket', '/path', '', '', ''))
        assert_equal(urlparse('s3://bucket/path#customname'),
                     ('s3', 'bucket', '/path#customname', '', '', ''))
        assert_equal(urlparse('s3://bucket'),
                     ('s3', 'bucket', '', '', '', ''))
        assert_equal(urlparse('s3://bucket/'),
                     ('s3', 'bucket', '/', '', '', ''))
