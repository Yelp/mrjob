# -*- encoding: utf-8 -*-
# Copyright 2015-2017 Yelp
# Copyright 2018 Yelp
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
from mrjob.logs.task import _match_task_log_path
from mrjob.logs.wrap import _cat_log_lines
from mrjob.logs.wrap import _ls_logs

from tests.py2 import patch
from tests.py2 import Mock
from tests.sandbox import BasicTestCase


class CatLogsTestCase(BasicTestCase):

    def setUp(self):
        super(CatLogsTestCase, self)

        self.mock_data = None

        self.mock_fs = Mock()
        self.mock_fs.cat = Mock(return_value=())
        self.mock_fs.exists = Mock(return_value=True)

        self.mock_log = self.start(patch('mrjob.logs.wrap.log'))

    # wrapper for cat_log() that uses self.mock_fs and turns result into list
    def cat_log(self, path):
        return list(_cat_log_lines(self.mock_fs, path))

    def test_yields_lines(self):
        self.mock_fs.cat.return_value = (
            chunk for chunk in [b'ba', b'r\nb', b'az'])

        self.assertEqual(self.cat_log('foo'), ['bar\n', 'baz'])

        self.mock_fs.exists.assert_called_once_with('foo')
        self.mock_fs.cat.assert_called_once_with('foo')
        self.assertFalse(self.mock_log.warning.called)

    def test_nonexistent_path(self):
        self.mock_fs.exists.return_value = False

        self.assertEqual(self.cat_log('foo'), [])

        self.mock_fs.exists.assert_called_once_with('foo')
        self.assertFalse(self.mock_fs.cat.called)
        self.assertFalse(self.mock_log.warning.called)

    def _test_recoverable_error(self, ex):
        self.mock_fs.cat.side_effect = ex

        self.assertEqual(self.cat_log('foo'), [])

        self.mock_fs.exists.assert_called_once_with('foo')
        self.mock_fs.cat.assert_called_once_with('foo')
        self.assertTrue(self.mock_log.warning.called)

    def test_cat_ioerror(self):
        self._test_recoverable_error(IOError())

    def test_cat_bad_ssh_binary(self):
        # tests #1474
        self._test_recoverable_error(OSError(2, 'No such file or directory'))

    def test_cat_other_error(self):
        self.mock_fs.cat.side_effect = ValueError

        self.assertRaises(ValueError, self.cat_log, 'foo')

        self.mock_fs.exists.assert_called_once_with('foo')
        self.mock_fs.cat.assert_called_once_with('foo')
        self.assertFalse(self.mock_log.warning.called)

    def test_exists_ioerror(self):
        self.mock_fs.exists.side_effect = IOError

        self.assertEqual(self.cat_log('foo'), [])

        self.mock_fs.exists.assert_called_once_with('foo')
        self.assertFalse(self.mock_fs.cat.called)
        self.assertTrue(self.mock_log.warning.called)

    def test_exists_other_error(self):
        self.mock_fs.exists.side_effect = ValueError

        self.assertRaises(ValueError, self.cat_log, 'foo')

        self.mock_fs.exists.assert_called_once_with('foo')
        self.assertFalse(self.mock_fs.cat.called)
        self.assertFalse(self.mock_log.warning.called)


class LsLogsTestCase(BasicTestCase):

    def setUp(self):
        super(LsLogsTestCase, self).setUp()

        self.mock_fs = Mock()
        self.mock_paths = []

        def mock_fs_ls(log_dir):
            prefix = log_dir.rstrip('/') + '/'

            exists = False

            for p in self.mock_paths:
                if isinstance(p, Exception):
                    raise p
                elif p.startswith(prefix):
                    yield p
                    exists = True

            if not exists:
                raise IOError

        def mock_fs_exists(log_dir):
            return any(mock_fs_ls(log_dir))

        self.mock_fs.ls = Mock(side_effect=mock_fs_ls)
        self.mock_fs.exists = Mock(side_effect=mock_fs_exists)

        # a matcher that cheerfully passes through kwargs
        def mock_matcher(path, **kwargs):
            return dict(**kwargs)

        self.mock_matcher = Mock(side_effect=mock_matcher)

        self.log = self.start(patch('mrjob.logs.wrap.log'))

    def _ls_logs(self, log_dir_stream, **kwargs):
        """Call _ls_logs() with self.mock_fs and self.mock_matcher,
        and return a list."""
        return list(_ls_logs(
            self.mock_fs, log_dir_stream, self.mock_matcher, **kwargs))

    def test_no_log_dirs(self):
        self.assertEqual(self._ls_logs([]), [])

    def test_no_paths(self):
        self.assertEqual(self._ls_logs([['/path/to/logs']]), [])

        self.assertTrue(self.mock_fs.exists.called)
        self.assertFalse(self.mock_fs.ls.called)

    def test_log_dirs_cant_be_str(self):
        self.assertRaises(TypeError, self._ls_logs, ['/path/to/logs'])

    def test_paths(self):
        self.mock_paths = [
            '/path/to/logs/oak',
            '/path/to/logs/pine',
            '/path/to/logs/redwood',
        ]

        self.assertEqual(self._ls_logs([['/path/to/logs']]),
                         [dict(path='/path/to/logs/oak'),
                          dict(path='/path/to/logs/pine'),
                          dict(path='/path/to/logs/redwood')])

    def test_multiple_log_dirs(self):
        self.mock_paths = [
            'ssh://node1/logs/syslog',
            'ssh://node2/logs/syslog',
        ]

        self.assertEqual(
            self._ls_logs([['ssh://node1/logs/', 'ssh://node2/logs/']]),
            [dict(path='ssh://node1/logs/syslog'),
             dict(path='ssh://node2/logs/syslog')])

    def test_stop_after_match(self):
        self.mock_paths = [
            's3://bucket/logs/node1/syslog',
            's3://bucket/logs/node2/syslog',
            'ssh://node1/logs/syslog',
            'ssh://node2/logs/syslog',
        ]

        self.assertEqual(
            self._ls_logs([
                ['s3://bucket/logs'],
                ['ssh://node1/logs/', 'ssh://node2/logs/']]),
            [dict(path='s3://bucket/logs/node1/syslog'),
             dict(path='s3://bucket/logs/node2/syslog')])

    def test_sort_within_directories(self):
        self.mock_paths = [
            '/log/dir/userlogs/application_1450486922681_0004/'
            'container_1450486922681_0005_01_000003/stderr',
            '/log/dir/userlogs/application_1450486922681_0004/'
            'container_1450486922681_0005_01_000004/stderr',
            '/log/dir/userlogs2/application_1450486922681_0004/'
            'container_1450486922681_0005_01_000005/stderr',
            's3://bucket/logs/application_1450486922681_0004/'
            'container_1450486922681_0005_01_000003/stderr',
        ]

        self.mock_matcher = _match_task_log_path

        def _match(path_num):
            path = self.mock_paths[path_num]
            return dict(path=path, **_match_task_log_path(path))

        # self.mock_paths[2] is actually more recent, but it's in
        # the second directory.

        # shouldn't look at s3 at all, since that's in the second batch
        # of paths, and we already found our path in the first batch
        self.assertEqual(
            self._ls_logs([
                ['/log/dir/userlogs', '/log/dir/userlogs2'],
                ['s3://bucket/logs']]),
            [_match(1), _match(0), _match(2)])

    def test_matcher_can_filter(self):
        def matcher(path):
            if path.endswith('/syslog'):
                return {}
            else:
                return None

        self.mock_matcher.side_effect = matcher

        self.mock_paths = [
            's3://bucket/logs/main/stderr',
            'ssh://main/logs/stderr',
            'ssh://main/logs/syslog',
        ]

        self.assertEqual(
            self._ls_logs([['s3://bucket/logs'], ['ssh://main/logs/']]),
            [dict(path='ssh://main/logs/syslog')])

    def test_kwargs_passed_to_matcher(self):
        self.mock_paths = [
            '/path/to/logs/oak',
            '/path/to/logs/pine',
            '/path/to/logs/redwood',
        ]

        self.assertEqual(self._ls_logs([['/path/to/logs']], foo='bar'),
                         [dict(path='/path/to/logs/oak', foo='bar'),
                          dict(path='/path/to/logs/pine', foo='bar'),
                          dict(path='/path/to/logs/redwood', foo='bar')])

    def _test_recoverable_error(self, ex):
        self.mock_paths = [
            '/path/to/logs/oak',
            ex
        ]

        self.assertEqual(self._ls_logs([['/path/to/logs']]),
                         [dict(path='/path/to/logs/oak')])

        self.mock_fs.ls.assert_called_once_with('/path/to/logs')

        self.assertTrue(self.log.warning.called)

        warnings = [a[0] for a, kw in self.log.warning.call_args_list]

        self.assertTrue(warnings[0].startswith("couldn't ls() /path/to/logs"))

    def test_warn_on_io_error(self):
        self._test_recoverable_error(IOError())

    def test_warn_on_bad_ssh_binary(self):
        # tests #1474
        self._test_recoverable_error(OSError(2, 'No such file or directory'))

    def test_raise_other_exceptions(self):
        self.mock_paths = [
            '/path/to/logs/oak',
            ValueError(),
        ]

        self.assertRaises(ValueError, self._ls_logs, [['/path/to/logs']])
