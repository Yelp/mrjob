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

"""Classes and functions related to measuring the amount of time spent in 
various parts of the code.
"""

import functools
import resource


class Profiler(object):
    """Provide facilities for measuring time spent in user code (mappers/reducer) vs IO wait and framework."""

    def __init__(self):
        super(Profiler, self).__init__()
        self.last_measurement = resource.getrusage(resource.RUSAGE_SELF)
        self.accumulated_other_time = 0.0
        self.accumulated_processing_time = 0.0

    def add_time_to_other(self):
        """Transition from 'other' code to 'processing' code"""
        current_measurement = resource.getrusage(resource.RUSAGE_SELF)

        stime_delta = current_measurement.ru_stime - self.last_measurement.ru_stime
        utime_delta = current_measurement.ru_utime - self.last_measurement.ru_utime
        self.accumulated_other_time += stime_delta + utime_delta
        self.last_measurement = current_measurement

    def add_time_to_processing(self):
        """Transition from 'processing' code back to 'other' code"""
        new_measurement = resource.getrusage(resource.RUSAGE_SELF)
        stime_delta = new_measurement.ru_stime - self.last_measurement.ru_stime
        utime_delta = new_measurement.ru_utime - self.last_measurement.ru_utime
        self.accumulated_processing_time += stime_delta + utime_delta

    def results(self):
        """Quickly get measurements

        :return: ``accumulated_processing_time``, ``accumulated_other_time``
        """
        return self.accumulated_processing_time, self.accumulated_other_time

    def _wrap_normal(self, func, mark_begin, mark_end):
        # wrap a normal function, nothing too fancy here
        # except to use @wraps to preserve the function signature
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            mark_begin()
            result = func(*args, **kwargs)
            mark_end()
            return result
        return wrapper

    def _wrap_generator(self, gen, mark_begin, mark_end):
        # Generators should be measured each time they yield.
        @functools.wraps(gen)
        def wrapper(*args, **kwargs):
            mark_begin()
            for item in gen(*args, **kwargs):
                mark_end()
                yield item
                # yield only continues in the current scope when the caller
                # is ready, so mark_begin() is safe here.
                mark_begin()
            mark_end()
        return wrapper

    def wrap_processing(self, processing_func, generator=False):
        """Wrap a function in "processing" markers.

        :type processing_func: function
        :param processing_func: function to wrap in "processing" markers
        :type generator: bool
        :param generator: set to ``True`` if ``processing_func`` is a generator so its iterations can be counted towards processing time
        """
        # this is broken out in case we decide to also have a wrap_other() function
        if generator:
            return self._wrap_generator(processing_func,
                                        self.add_time_to_other,
                                        self.add_time_to_processing)
        else:
            return self._wrap_normal(processing_func,
                                     self.add_time_to_other,
                                     self.add_time_to_processing)
