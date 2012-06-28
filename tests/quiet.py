# Copyright 2011 Yelp
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

"""Utility functions for turning off printouts during testing."""

from contextlib import contextmanager
import logging
from StringIO import StringIO


# this exists as logging.NullHandler as of Python 2.7
class NullHandler(logging.Handler):
    def emit(self, record):
        pass


def log_to_buffer(name=None, level=logging.WARNING):
    buf = StringIO()
    log = logging.getLogger(name)
    log.addHandler(logging.StreamHandler(buf))
    log.setLevel(level)
    return buf


@contextmanager
def logger_disabled(name=None):
    """Temporarily disable a logger.

    Use this in a `with` block. For example::

        with logger_disabled('mrjob.conf'):
            find_mrjob_conf()  # this would normally log stuff
    """
    log = logging.getLogger(name)
    was_disabled = log.disabled
    log.disabled = True

    yield

    log.disabled = was_disabled


@contextmanager
def no_handlers_for_logger(name=None):
    """Temporarily remove handlers all handlers from a logger. Useful for that
    rare case when we need a logger to work, but we don't want to get printouts
    to the command line.

    Use this in a `with` block. For example::

        mr_job.sandbox()

        with no_handlers_for_logger('mrjob.local'):
            mr_job.run_job()

        ...  # look for logging messages inside mr_job.stderr

    Any handlers you add inside the `with` block will be removed at the end.
    """
    log = logging.getLogger(name)
    old_handlers = log.handlers
    old_propagate = log.propagate

    # add null handler so logging doesn't yell about there being no handlers
    log.handlers = [NullHandler()]

    yield

    # logging module logic for setting handlers and propagate is opaque.
    # Setting both effectively ends with propagate = 0 in all cases.
    # We just want to avoid 'no handlers for logger...' junk messages in tests
    # cases.
    if old_handlers:
        log.handlers = old_handlers
    else:
        log.propagate = old_propagate
