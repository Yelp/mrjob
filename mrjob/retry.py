# Copyright 2009-2013 Yelp, David Marin
# Copyright 2015 Yelp
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
"""Wrappers for gracefully retrying on error."""
import logging
import time

log = logging.getLogger(__name__)


class RetryWrapper(object):
    """Handle transient errors, with configurable backoff.

    This class can wrap any object. The wrapped object will behave like
    the original one, except that if you call a function and it raises a
    retriable exception, we'll back off for a certain number of seconds
    and call the function again, until it succeeds or we get a non-retriable
    exception.
    """
    # TODO: this doesn't correctly handle object properties or wrapping
    # functions.
    def __init__(self, wrapped, retry_if, backoff=15, multiplier=1.5,
                 max_tries=10):
        """
        Wrap the given object

        :param wrapped: the object to wrap
        :param retry_if: a method that takes an exception, and returns whether
                         we should retry
        :type backoff: float
        :param backoff: the number of seconds to wait the first time we get a
                        retriable error
        :type multiplier: float
        :param multiplier: if we retry multiple times, the amount to multiply
                           the backoff time by every time we get an error
        :type max_tries: int
        :param max_tries: how many tries we get. ``0`` means to keep trying
                          forever
        """
        self.__wrapped = wrapped

        self.__retry_if = retry_if

        self.__backoff = backoff
        if self.__backoff <= 0:
            raise ValueError('backoff must be positive')

        self.__multiplier = multiplier
        if self.__multiplier < 1:
            raise ValueError('multiplier must be at least one!')

        self.__max_tries = max_tries

    def __getattr__(self, name):
        """The glue that makes functions retriable, and returns other
        attributes from the wrapped object as-is."""
        x = getattr(self.__wrapped, name)
        if hasattr(x, '__call__'):
            return self.__wrap_method_with_call_and_maybe_retry(x)
        else:
            return x

    def __wrap_method_with_call_and_maybe_retry(self, f):
        """Wrap method f in a retry loop."""

        def call_and_maybe_retry(*args, **kwargs):
            backoff = self.__backoff
            tries = 0

            while (not self.__max_tries or tries < self.__max_tries):
                try:
                    return f(*args, **kwargs)
                except Exception as ex:
                    if (self.__retry_if(ex) and
                        (tries < self.__max_tries - 1 or
                         not self.__max_tries)):
                        log.info('Got retriable error: %r' % ex)
                        log.info('Backing off for %.1f seconds' % backoff)
                        time.sleep(backoff)
                        tries += 1
                        backoff *= self.__multiplier
                    else:
                        raise

        # pretend to be the original function
        if hasattr(f, '__name__'):
            call_and_maybe_retry.__name__ == f.__name__
        return call_and_maybe_retry
