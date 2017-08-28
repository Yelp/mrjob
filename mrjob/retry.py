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
from functools import wraps

log = logging.getLogger(__name__)


class RetryGoRound(object):
    """Handle flaky mirrors/endpoints by trying them all.

    This wraps two or more objects; if a call on the first one fails in a
    non-fatal way, try the next one and so on until success, or until
    we've tried all the alternatives.

    This doesn't currently support backoff; combine it with RetryWrapper
    for that.
    """
    def __init__(self, alternatives, retry_if):
        """Wrap the given objects

        :type alternatives: list
        :param alternatives: the objects to wrap
        :param retry_if: a method that takes an exception, and returns whether
                         we should retry
        """
        if not alternatives:
            raise ValueError('must provide at least one alternative')
        self.__alternatives = tuple(alternatives)  # don't allow modifying list

        self.__retry_if = retry_if

        # where to start trying from
        self.__start_index = 0

    def __getattr__(self, name):
        """The glue that makes functions retriable, and returns other
        attributes from the wrapped object as-is."""
        # use whichever alternative last worked as our model
        x = getattr(self.__alternatives[self.__start_index], name)
        if hasattr(x, '__call__'):
            return self.__wrap_methods_with_call_and_maybe_retry(name)
        else:
            return x

    def __wrap_methods_with_call_and_maybe_retry(self, name):
        """Wrap calls to name()."""

        def call_and_maybe_retry(*args, **kwargs):
            n = len(self.__alternatives)

            for i in range(n):
                index = (self.__start_index + i) % n
                alternative = self.__alternatives[index]

                try:
                    result = getattr(alternative, name)(*args, **kwargs)
                    # this one works, start here next time!
                    self.__start_index = index
                    return result
                except Exception as ex:
                    # that one didn't work out, retry if we can
                    if i < n - 1 and self.__retry_if(ex):
                        log.info('%r.%s() raised %r, trying next alternative'
                                 % (alternative, name, ex))
                    else:
                        raise

        # pretend to be the original function
        f = getattr(self.__alternatives[self.__start_index], name)
        if hasattr(f, '__name__'):
            return wraps(f)(call_and_maybe_retry)
        else:
            return f


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
