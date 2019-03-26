# Copyright 2009-2012 Yelp and Contributors
# Copyright 2013 David Marin
# Copyright 2015-2018 Yelp
# Copyright 2019 Yelp
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
import logging
import os
import os.path
import random
import stat
from contextlib import contextmanager
from tempfile import mkdtemp
from shutil import rmtree
from unittest import TestCase
from unittest import skipIf
from warnings import filterwarnings

try:
    import pyspark
except ImportError:
    pyspark = None

import mrjob
from mrjob import runner
from mrjob.py2 import PY2
from mrjob.util import NullHandler

from tests.py2 import patch


# simple config that also silences 'no config options for runner' logging
EMPTY_MRJOB_CONF = {'runners': {
    'local': {
        'label': 'test_job',
    },
    'emr': {
        'check_cluster_every': 0.00,
        'cloud_fs_sync_secs': 0.00,
    },
    'hadoop': {
        'label': 'test_job',
    },
    'inline': {
        'label': 'test_job',
    },
    'dataproc': {
        'api_cooldown_secs': 0.00,
        'cloud_fs_sync_secs': 0.00
    }
}}


def mrjob_conf_patcher(substitute_conf=EMPTY_MRJOB_CONF):
    def mock_load_opts_from_mrjob_confs(runner_alias, conf_paths=None):
        return [(None,
                 substitute_conf.get('runners', {}).get(runner_alias, {}))]

    return patch.object(runner, 'load_opts_from_mrjob_confs',
                        mock_load_opts_from_mrjob_confs)


@contextmanager
def random_seed(seed):
    """Temporarily change the seed of the random number generator."""
    state = random.getstate()

    random.seed(seed)

    try:
        yield
    finally:
        random.setstate(state)


class BasicTestCase(TestCase):
    """All mrjob tests should inherit from this or a subclass, so we don't
    get extraneous logging messages while running the tests.

    This also adds the :py:meth:`start` method, which insures that patches
    are always reverted after the test completes (e.g.
     ``self.start(patch('foo.bar', ...))`)
    """
    def setUp(self):
        """disable all logging handlers
        """
        # Extra logging messages were cluttering Travis CI. See #1793
        super(BasicTestCase, self).setUp()

        for name in ['', '__main__', 'mrjob']:
            log = logging.getLogger(name)
            self.start(patch.object(log, 'handlers', []))

            if not name:
                # add a dummy handler to the root logger
                log.addHandler(NullHandler())

    def start(self, patcher):
        """Add the given patcher to this test case's cleanup actions,
        then start it, and return the mock it returns. Example:

        mock_turtle = self.start(patch('foo.bar.turtle'))
        """
        mock = patcher.start()
        # don't addCleanup() if patcher fails
        self.addCleanup(patcher.stop)
        return mock


class EmptyMrjobConfTestCase(BasicTestCase):

    # set to None if you don't want load_opts_from_mrjob_confs patched
    MRJOB_CONF_CONTENTS = EMPTY_MRJOB_CONF

    def setUp(self):
        super(EmptyMrjobConfTestCase, self).setUp()

        if self.MRJOB_CONF_CONTENTS is not None:
            self.mrjob_conf_patcher = mrjob_conf_patcher(
                self.MRJOB_CONF_CONTENTS)
            self.mrjob_conf_patcher.start()
            self.addCleanup(self.mrjob_conf_patcher.stop)


class SandboxedTestCase(EmptyMrjobConfTestCase):
    """Patch mrjob.conf, create a temp directory, and save the environment for
    each test
    """
    def setUp(self):
        super(SandboxedTestCase, self).setUp()

        # tmp dir
        self.tmp_dir = mkdtemp()
        self.addCleanup(rmtree, self.tmp_dir)

        # environment
        old_environ = os.environ.copy()
        # cleanup functions are called in reverse order
        self.addCleanup(os.environ.update, old_environ)
        self.addCleanup(os.environ.clear)

    def makedirs(self, path):
        abs_path = os.path.join(self.tmp_dir, path)
        if not os.path.isdir(abs_path):
            os.makedirs(abs_path)
        return abs_path

    def makefile(self, path, contents=b'', executable=False):
        self.makedirs(os.path.dirname(path))
        abs_path = os.path.join(self.tmp_dir, path)

        mode = 'wb' if isinstance(contents, bytes) else 'w'
        with open(abs_path, mode) as f:
            f.write(contents)
        if executable:
            os.chmod(abs_path,
                     os.stat(abs_path).st_mode | stat.S_IXUSR)

        return abs_path

    def abs_paths(self, *paths):
        return [os.path.join(self.tmp_dir, path) for path in paths]

    def add_mrjob_to_pythonpath(self):
        """call this for tests that are going to invoke a subprocess
        that needs to find mrjob.

        (Merely using the local runner won't require this, because it
        bootstraps mrjob by default.)
        """
        os.environ['PYTHONPATH'] = (
            mrjob_pythonpath() + ':' + os.environ.get('PYTHONPATH', ''))


@skipIf(pyspark is None, 'no pyspark module')
class SingleSparkContextTestCase(BasicTestCase):
    """Ensure that each test case gets a fresh SparkContext and JVM.
    """
    def stop_active_spark_context(self):
        from pyspark import SparkContext

        with SparkContext._lock:
            if SparkContext._active_spark_context:
                SparkContext._active_spark_context.stop()

    def quiet_spark_context_logging(self):
        from pyspark import SparkContext

        def quiet_SparkContext(*args, **kwargs):
            sc = SparkContext(*args, **kwargs)
            sc.setLogLevel('FATAL')

            return sc

        self.start(patch('pyspark.SparkContext',
                         side_effect=quiet_SparkContext))

    def setUp(self):
        super(SingleSparkContextTestCase, self).setUp()

        if not PY2:
            # ignore Python 3 warnings about unclosed filehandles
            # (unittest resets warnings for every test)
            filterwarnings('ignore', category=ResourceWarning)

        self.addCleanup(self.stop_active_spark_context)
        self.stop_active_spark_context()


def mrjob_pythonpath():
    """The directory containing the mrjob package that we've imported."""
    return os.path.abspath(
        os.path.join(os.path.dirname(mrjob.__file__), '..'))
