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

"""Tests of all the amazing utilities in mrjob.util"""

from StringIO import StringIO
from testify import TestCase, assert_equal, assert_gt, assert_lt, assert_in, assert_raises, class_setup, class_teardown, setup, teardown

from mrjob.profile import *
from tests.mr_two_step_job import MRTwoStepJob


class BasicsTestCase(TestCase):

    @setup
    def make_profiler(self):
        self.profiler = Profiler()

    def make_useless_function(self, n):
        def func():
            for i in range(n):
                pass
        return func

    def make_useless_generator(self, n, k):
        each_iter = self.make_useless_function(k)
        def gen():
            for i in range(n):
                each_iter()
                yield i
        return gen

    def test_timing_func_expensive(self):
        func = self.profiler.wrap_processing(self.make_useless_function(1000000))
        func()
        user_time, other_time = self.profiler.results()
        assert_gt(user_time, other_time)

    def test_timing_func_cheap(self):
        func = self.profiler.wrap_processing(self.make_useless_function(1000))
        func()
        self.make_useless_function(10000000)()
        self.profiler.add_time_to_other()
        user_time, other_time = self.profiler.results()
        assert_lt(user_time, other_time)



class CommandLineTestCase(TestCase):

    def test_profiling(self):
        stdin = StringIO('foo\nbar\n')
        mr_job = MRTwoStepJob(['-r', 'local', '--profile', 
                              '--no-conf'])
        mr_job.sandbox(stdin=stdin)
        with mr_job.make_runner() as runner:
            runner.run()
            assert_in('profile', runner._counters[0])
