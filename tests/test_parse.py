# -*- encoding: utf-8 -*-
# Copyright 2009-2012 Yelp
# Copyright 2013 Steve Johnson
# Copyright 2014 Phil Swanson and Marc Abramowitz
# Copyright 2015-2016 Yelp
# Copyright 2017 Yelp and Contributors
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
from unittest import TestCase

from mrjob.parse import _parse_port_range_list
from mrjob.parse import _parse_progress_from_job_tracker
from mrjob.parse import _parse_progress_from_resource_manager
from mrjob.parse import is_s3_uri
from mrjob.parse import is_uri
from mrjob.parse import parse_mr_job_stderr
from mrjob.parse import parse_s3_uri
from mrjob.parse import urlparse


class ParseMRJobStderrTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(parse_mr_job_stderr(BytesIO()),
                         {'counters': {}, 'statuses': [], 'other': []})

    def test_parsing(self):
        INPUT = BytesIO(
            b'reporter:counter:Foo,Bar,2\n' +
            b'reporter:status:Baz\n' +
            b'reporter:status:Baz\n' +
            b'reporter:counter:Foo,Bar,1\n' +
            b'reporter:counter:Foo,Baz,1\n' +
            b'reporter:counter:Quux Subsystem,Baz,42\n' +
            b'Warning: deprecated metasyntactic variable: garply\n')

        self.assertEqual(
            parse_mr_job_stderr(INPUT),
            {'counters': {'Foo': {'Bar': 3, 'Baz': 1},
                          'Quux Subsystem': {'Baz': 42}},
             'statuses': ['Baz', 'Baz'],
             'other': ['Warning: deprecated metasyntactic variable: garply\n']
            })

    def test_update_counters(self):
        counters = {'Foo': {'Bar': 3, 'Baz': 1}}

        parse_mr_job_stderr(
            BytesIO(b'reporter:counter:Foo,Baz,1\n'), counters=counters)

        self.assertEqual(counters, {'Foo': {'Bar': 3, 'Baz': 2}})

    def test_read_single_line(self):
        # LocalMRJobRunner runs parse_mr_job_stderr on one line at a time.
        self.assertEqual(parse_mr_job_stderr(b'reporter:counter:Foo,Bar,2\n'),
                         {'counters': {'Foo': {'Bar': 2}},
                          'statuses': [], 'other': []})

    def test_read_multiple_lines_from_buffer(self):
        self.assertEqual(
            parse_mr_job_stderr(b'reporter:counter:Foo,Bar,2\nwoot\n'),
            {'counters': {'Foo': {'Bar': 2}},
             'statuses': [], 'other': ['woot\n']})

    def test_negative_counters(self):
        # kind of poor practice to use negative counters, but Hadoop
        # Streaming supports it (negative numbers are integers too!)
        self.assertEqual(
            parse_mr_job_stderr([b'reporter:counter:Foo,Bar,-2\n']),
            {'counters': {'Foo': {'Bar': -2}},
             'statuses': [], 'other': []})

    def test_garbled_counters(self):
        # we should be able to do something graceful with
        # garbled counters and status messages
        BAD_LINES = [
            b'reporter:counter:Foo,Bar,Baz,1\n',  # too many items
            b'reporter:counter:Foo,1\n',  # too few items
            b'reporter:counter:Foo,Bar,a million\n',  # not a number
            b'reporter:counter:Foo,Bar,1.0\n',  # not an int
            b'reporter:crounter:Foo,Bar,1\n',  # not a valid reporter
            b'reporter,counter:Foo,Bar,1\n',  # wrong format!
        ]

        self.assertEqual(
            parse_mr_job_stderr(BAD_LINES),
            {'counters': {}, 'statuses': [],
             'other': [line.decode('ascii') for line in BAD_LINES]})


class PortRangeListTestCase(TestCase):
    def test_port_range_list(self):
        self.assertEqual(_parse_port_range_list('1234'), [1234])
        self.assertEqual(_parse_port_range_list('123,456,789'), [123, 456, 789])
        self.assertEqual(_parse_port_range_list('1234,5678'), [1234, 5678])
        self.assertEqual(_parse_port_range_list('1234:1236'),
                         [1234, 1235, 1236])
        self.assertEqual(_parse_port_range_list('123:125,456'),
                         [123, 124, 125, 456])
        self.assertEqual(_parse_port_range_list('123:125,456:458'),
                         [123, 124, 125, 456, 457, 458])
        self.assertEqual(_parse_port_range_list('0123'), [123])

        self.assertRaises(ValueError, _parse_port_range_list, 'Alexandria')
        self.assertRaises(ValueError, _parse_port_range_list,
                          'Athens:Alexandria')


class URITestCase(TestCase):
    def test_is_uri(self):
        self.assertEqual(is_uri('notauri!'), False)
        self.assertEqual(is_uri('they://did/the/monster/mash'), True)
        self.assertEqual(is_uri('C:\some\windows\path'), False)
        # test #1455
        self.assertEqual(is_uri('2016-10-11T06:29:17'), False)
        # sorry, we only care about file URIs
        self.assertEqual(is_uri('mailto:someone@example.com'), False)
        # urlparse has to accept it
        self.assertEqual(is_uri('://'), False)

    def test_is_s3_uri(self):
        self.assertEqual(is_s3_uri('s3://a/uri'), True)
        self.assertEqual(is_s3_uri('s3n://a/uri'), True)
        self.assertEqual(is_s3_uri('s3a://a/uri'), True)
        self.assertEqual(is_s3_uri('hdfs://a/uri'), False)

    def test_parse_s3_uri(self):
        self.assertEqual(parse_s3_uri('s3://bucket/loc'), ('bucket', 'loc'))

    def test_urlparse(self):
        self.assertEqual(urlparse('http://www.yelp.com/lil_brudder'),
                         ('http', 'www.yelp.com', '/lil_brudder', '', '', ''))
        self.assertEqual(urlparse('cant://touch/this'),
                         ('cant', 'touch', '/this', '', '', ''))
        self.assertEqual(urlparse('s3://bucket/path'),
                         ('s3', 'bucket', '/path', '', '', ''))
        self.assertEqual(urlparse('s3://bucket/path#customname'),
                         ('s3', 'bucket', '/path', '', '', 'customname'))
        self.assertEqual(urlparse('s3://bucket'),
                         ('s3', 'bucket', '', '', '', ''))
        self.assertEqual(urlparse('s3://bucket/'),
                         ('s3', 'bucket', '/', '', '', ''))


class JobTrackerProgressTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(_parse_progress_from_job_tracker(b''), (None, None))

    def test_on_html_snippet(self):
        HTML = b"""
  <ul id="quicklinks-list">
    <li><a href="#scheduling_info">Scheduling Info</a></li>
    <li><a href="#running_jobs">Running Jobs</a></li>
    <li><a href="#retired_jobs">Retired Jobs</a></li>
    <li><a href="#local_logs">Local Logs</a></li>
  </ul>

<h2 id="running_jobs">Running Jobs</h2>
<table border="1" cellpadding="5" cellspacing="0" class="sortable" style="margin-top: 10px">
<thead><tr><td><b>Jobid</b></td><td><b>Started</b></td><td><b>Priority</b></td><td><b>User</b></td><td><b>Name</b></td><td><b>Map % Complete</b></td><td><b>Map Total</b></td><td><b>Maps Completed</b></td><td><b>Reduce % Complete</b></td><td><b>Reduce Total</b></td><td><b>Reduces Completed</b></td><td><b>Job Scheduling Information</b></td><td><b>Diagnostic Info </b></td></tr></thead>
<tbody><tr><td id="job_0"><a href="http://localhost:40426/jobdetails.jsp?jobid=job_201508212327_0003&refresh=30">job_201508212327_0003</a></td><td id="started_0">Fri Aug 21 23:32:49 UTC 2015</td><td id="priority_0" sorttable_customkey="2">NORMAL</td><td id="user_0">hadoop</td><td id="name_0">streamjob7446105887001298606.jar</td><td>27.51%<table border="1px" width="80px"><tbody><tr><td cellspacing="0" class="perc_filled" width="50%"></td><td cellspacing="0" class="perc_nonfilled" width="50%"></td></tr></tbody></table></td><td>4</td><td>2</td><td>0.00%<table border="1px" width="80px"><tbody><tr><td cellspacing="0" class="perc_nonfilled" width="100%"></td></tr></tbody></table></td><td>1</td><td> 0</td><td>NA</td><td>NA</td></tr>
</tbody><tfoot></tfoot></table>
        """
        self.assertEqual(_parse_progress_from_job_tracker(HTML),
                         (27.51, 0))

    def test_ignore_complete_jobs(self):
        # regression test for #793
        HTML = b"""
  <ul id="quicklinks-list">
    <li><a href="#scheduling_info">Scheduling Info</a></li>
    <li><a href="#running_jobs">Running Jobs</a></li>
    <li><a href="#retired_jobs">Retired Jobs</a></li>
    <li><a href="#local_logs">Local Logs</a></li>
  </ul>

  <h2 id="running_jobs">Running Jobs</h2>
<table border="1" cellpadding="5" cellspacing="0">
<tr><td align="center" colspan="8"><i>none</i></td></tr>
</table>

<hr>

<h2 id="completed_jobs">Completed Jobs</h2><table border="1" cellpadding="5" cellspacing="0" class="sortable" style="margin-top: 10px">
<thead><tr><td><b>Jobid</b></td><td><b>Started</b></td><td><b>Priority</b></td><td><b>User</b></td><td><b>Name</b></td><td><b>Map % Complete</b></td><td><b>Map Total</b></td><td><b>Maps Completed</b></td><td><b>Reduce % Complete</b></td><td><b>Reduce Total</b></td><td><b>Reduces Completed</b></td><td><b>Job Scheduling Information</b></td><td><b>Diagnostic Info </b></td></tr></thead>
<tr><td id="job_0"><a href="jobdetails.jsp?jobid=job_201611042349_0003&refresh=0">job_201611042349_0003</a></td><td id="started_0">Sat Nov 05 00:20:55 UTC 2016</td><td id="priority_0" sorttable_customkey="2">NORMAL</td><td id="user_0">hadoop</td><td id="name_0">streamjob4785184554048208079.jar</td><td>100.00%<table border="1px" width="80px"><tr><td cellspacing="0" class="perc_filled" width="100%"></td></tr></table></td><td>4</td><td>4</td><td>100.00%<table border="1px" width="80px"><tr><td cellspacing="0" class="perc_filled" width="100%"></td></tr></table></td><td>1</td><td> 1</td><td>NA</td><td>NA</td></tr>
</table>
        """
        self.assertEqual(_parse_progress_from_job_tracker(HTML),
                         (None, None))


class ResourceManagerProgressTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(_parse_progress_from_resource_manager(b''), None)

    def test_partially_complete_job(self):
        # the actual data is in JavaScript at the bottom of the page
        JS = b"""
<script type="text/javascript">
              var appsTableData=[
["<a href='/cluster/app/application_1440199050012_0002'>application_1440199050012_0002</a>","hadoop","streamjob4609242403924457306.jar","MAPREDUCE","default","1440199276424","1440199351438","FINISHED","SUCCEEDED","<br title='100.0'> <div class='ui-progressbar ui-widget ui-widget-content ui-corner-all' title='100.0%'> <div class='ui-progressbar-value ui-widget-header ui-corner-left' style='width:100.0%'> </div> </div>","<a href='http://172.31.23.88:9046/proxy/application_1440199050012_0002/jobhistory/job/job_1440199050012_0002'>History</a>"],
["<a href='/cluster/app/application_1440199050012_0003'>application_1440199050012_0003</a>","hadoop","streamjob1426116009682801380.jar","MAPREDUCE","default","1440205192909","0","RUNNING","UNDEFINED","<br title='5.0'> <div class='ui-progressbar ui-widget ui-widget-content ui-corner-all' title='5.0%'> <div class='ui-progressbar-value ui-widget-header ui-corner-left' style='width:5.0%'> </div> </div>","<a href='http://172.31.23.88:9046/proxy/application_1440199050012_0003/'>ApplicationMaster</a>"],
["<a href='/cluster/app/application_1440199050012_0001'>application_1440199050012_0001</a>","hadoop","streamjob7935208784309830219.jar","MAPREDUCE","default","1440199122680","1440199195931","FINISHED","SUCCEEDED","<br title='100.0'> <div class='ui-progressbar ui-widget ui-widget-content ui-corner-all' title='100.0%'> <div class='ui-progressbar-value ui-widget-header ui-corner-left' style='width:100.0%'> </div> </div>","<a href='http://172.31.23.88:9046/proxy/application_1440199050012_0001/jobhistory/job/job_1440199050012_0001'>History</a>"]
]
            </script>
            <tbody>
            </tbody>
          </table>
    </tbody>
  </table>
</html>
        """
        self.assertEqual(_parse_progress_from_resource_manager(JS), 5.0)

    def test_completed_job(self):
        JS = b"""
<script type="text/javascript">
              var appsTableData=[
["<a href='/cluster/app/application_1440199050012_0002'>application_1440199050012_0002</a>","hadoop","streamjob4609242403924457306.jar","MAPREDUCE","default","1440199276424","1440199351438","FINISHED","SUCCEEDED","<br title='100.0'> <div class='ui-progressbar ui-widget ui-widget-content ui-corner-all' title='100.0%'> <div class='ui-progressbar-value ui-widget-header ui-corner-left' style='width:100.0%'> </div> </div>","<a href='http://172.31.23.88:9046/proxy/application_1440199050012_0002/jobhistory/job/job_1440199050012_0002'>History</a>"],
["<a href='/cluster/app/application_1440199050012_0001'>application_1440199050012_0001</a>","hadoop","streamjob7935208784309830219.jar","MAPREDUCE","default","1440199122680","1440199195931","FINISHED","SUCCEEDED","<br title='100.0'> <div class='ui-progressbar ui-widget ui-widget-content ui-corner-all' title='100.0%'> <div class='ui-progressbar-value ui-widget-header ui-corner-left' style='width:100.0%'> </div> </div>","<a href='http://172.31.23.88:9046/proxy/application_1440199050012_0001/jobhistory/job/job_1440199050012_0001'>History</a>"]
]
            </script>
            <tbody>
            </tbody>
          </table>
    </tbody>
  </table>
</html>
        """
        self.assertEqual(_parse_progress_from_resource_manager(JS), None)

    def test_failed_job(self):
        JS = b"""
<script type="text/javascript">
              var appsTableData=[
["<a href='/cluster/app/application_1440199050012_0001'>application_1440199050012_0001</a>","hadoop","streamjob7935208784309830219.jar","MAPREDUCE","default","1440199122680","1440199195931","FINISHED","FAILED","<br title='100.0'> <div class='ui-progressbar ui-widget ui-widget-content ui-corner-all' title='100.0%'> <div class='ui-progressbar-value ui-widget-header ui-corner-left' style='width:100.0%'> </div> </div>","<a href='http://172.31.23.88:9046/proxy/application_1440199050012_0001/jobhistory/job/job_1440199050012_0001'>History</a>"]
]
            </script>
            <tbody>
            </tbody>
          </table>
    </tbody>
  </table>
</html>
        """
        self.assertEqual(_parse_progress_from_resource_manager(JS), None)
