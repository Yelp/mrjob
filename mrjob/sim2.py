# Copyright 2009-2013 Yelp and Contributors
# Copyright 2015-2017 Yelp
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
import itertools
import logging
import os
import shutil
import stat
from multiprocessing import cpu_count
from os.path import join


from mrjob.options import _allowed_keys
from mrjob.options import _combiners
from mrjob.options import _deprecated_aliases
from mrjob.runner import RunnerOptionStore
from mrjob.util import unarchive


log = logging.getLogger(__name__)


class Sim2RunnerOptionStore(RunnerOptionStore):
    # these are the same for 'local' and 'inline' runners
    ALLOWED_KEYS = _allowed_keys('local')
    COMBINERS = _combiners('local')
    DEPRECATED_ALIASES = _deprecated_aliases('local')


    def __init__(self, **kwargs):
        super(Sim2RunnerOptionStore, self).__init__(**kwargs)

        self._counters = []

    # re-implement these in your subclass

    def _run_multiple_tasks(self, num_processes=None):
        """Run multiple tasks, possibly in parallel. Tasks are tuples of
        ``(func, args, kwds, callback)``, just like the arguments to
         :py:meth:`multiprocessing.Pool.apply_async`.
        """
        raise NotImplementedError

    def _run_task(self, task_type, input_split, step_num, task_num):
        """Run one mapper, reducer, or combiner."""
        raise NotImplementedError

    def _run(self):
        self._warn_ignored_opts()
        self._check_input_exists()
        self._create_setup_wrapper_script(local=True)
        self._setup_output_dir()

        last_output_paths = self._input_paths

        # run mapper, combiner, sort, reducer for each step
        for step_num, step in enumerate(self._get_steps()):
            log.info('Running step %d of %d...' % (
                step_num + 1, self._num_steps()))

            self._create_dist_cache_dir(step_num)

            map_splits = self._split_mapper_input(last_output_paths, step_num)

            last_output_paths = self._run_mappers_and_combiners(
                map_splits, step_num)

            if 'reducer' in step:
                reducer_splits = self._split_reducer_input(
                    last_output_paths, step_num)

                last_output_paths = self._run_reducers(
                    reducer_splits, step_num)

        # either copy to output dir, or have steps handle it

    def _run_mappers_and_combiners(self, map_splits, step_num):
        # TODO: possibly catch step failure
        return self._run_multiple_tasks(
            (self._run_mapper_and_combiner, (map_split, step_num, task_num))
            for task_num, map_split in enumerate(map_splits)
        )

    def _run_mapper_and_combiner(self, map_split, step_num, task_num):
        last_output_path = self._run_task(
            'mapper', map_split, step_num, task_num)

        step = self._get_step(step_num)
        if 'combiner' in step:
            last_output_path = self._sort_combiner_input(
                last_output_path, step_num, task_num)

            # TODO: do combiners see map.input.file etc.?
            combiner_split = dict(map_split)
            combiner_split['path'] = last_output_path

            last_output_path = self._run_task(
                'combiner', combiner_split, step_num, task_num)

        return last_output_path

    def _check_input_exists(self):
        if not self._opts['check_input_paths']:
            return

        for path in self._input_paths:
            # '-' (stdin) always exists
            if path != '-' and not self.fs.exists(path):
                raise AssertionError(
                    'Input path %s does not exist!' % (path,))

    def _create_dist_cache_dir(self, step_num):
        """Copy working directory files into a shared directory,
        simulating the way Hadoop's Distributed Cache works on nodes."""
        cache_dir_path = self._dist_cache_dir_path(step_num)
        os.mkdir(cache_dir_path)

        for name, path in self._working_dir_mgr.name_to_path('file').items():

            dest = self._path_in_dist_cache_dir(name, step_num)
            shutil.copy(path, dest)
            _chmod_u_rx(dest)

        for name, path in self._working_dir_mgr.name_to_path(
                'archive').items():

            dest = self._path_in_dist_cache_dir(name, step_num)
            unarchive(path, dest)
            _chmod_u_rx(dest, recursive=True)

    def _num_mappers(self, step_num):
        # TODO: look up mapred.job.maps (convert to int) in _jobconf_for_step()
        return cpu_count()

    def _num_reducers(self, step_num):
        # TODO: look up mapred.job.reduces in _jobconf_for_step()
        return cpu_count()

    def _split_mapper_input(self, input_paths, step_num):
        # goals:
        #
        # make enough splits to have 2x as many splits as mappers
        # leave compressed files alone
        # yield "splits" referring back to original file
        input_paths = list(input_paths)

        target_num_splits = self._num_mappers(step_num) * 2

        # decide on a split size to approximate target_num_splits
        num_compressed = 0
        uncompressed_bytes = 0

        for path in input_paths:
            if path.endswith('.gz') or path.endswith('.bz'):
                num_compressed += 1
            else:
                uncompressed_bytes += os.stat(path)[stat.ST_SIZE]

        split_size = uncompressed_bytes // max(
            target_num_splits - num_compressed, 1)

        task_num_gen = itertools.count()

        def next_split_path(self):
            task_num = task_num_gen.next()
            task_dir = self._task_dir_path('mapper', step_num, task_num)
            self.fs.mkdir(task_dir)
            return join(task_dir, 'input')

        for path in input_paths:
            if self._is_compressed_file(path):
                split_path = next_split_path()
                size = os.stat(path)[stat.ST_SIZE]

                with self._open_compressed_file(path) as src:
                    with open(split_path, 'wb') as dest:
                        shutil.copyfileobj(src, dest)

                yield dict(
                    path=path,
                    split_path=split_path,
                    start=0,
                    length=size)
            else:
                start = 0
                length = 0

                for line in open(path, 'rb'):
                    pass









        # split files
        results = []

        def next_split_path():
            mapper_task_dir = self._task_dir_path(
                'mapper', step_num, len(results))
            os.mkdir(mapper_task_dir)
            return os.path.join(mapper_task_dir, 'input')

        for path in input_paths:
            if self._is_compressed_file(path):
                pass







    def _is_compressed_file(self, path):
        return path.endswith('.bz2') or path.endswith('.gz2')

    def _open_compressed_file(self, path):
        pass # TODO

    def _split_sorted_reducer_input(self, path, step_num):
        """Split a single file containing sorted reducer input into reducer
        input files, and return them."""
        task_num = 0








    # directory structure

    # cache/

    def _dist_cache_dir_path(self, step_num):
        return join(self._step_dir_path(step_num), 'cache')

    # cache/<name>

    def _path_in_dist_cache_dir(self, name, step_num):
        return join(self._dist_cache_dir_path(step_num), name)

    # step/<step_num>/

    def _step_dir_path(self, step_num):
        return join(self.tmp_dir, 'step', '%3d' % step_num)

    # step/<step_num>/<task_type>/<task_num>/

    def _task_dir_path(self, task_type, step_num, task_num):
        return join(self._step_dir_path(step_num), task_type, '%5d' % task_num)

    # step/<step_num>/<task_type>/<task_num>/input

    def _task_working_dir_path(self, task_type, step_num, task_num):
        return join(self._task_dir_path(task_type, step_num, task_num), 'wd')

    # need this specifically since there's a global sort of reducer input

    def _sorted_reducer_input_path(self, step_num):
        return join(self._step_dir_path(step_num), 'reducer', 'sorted-input')


def _chmod_u_rx(path, recursive=False):
    # TODO: implement this
    pass



def _split_lines(line_gen, split_size, reducer_key=None):
    pass

def _label_lines_for_split(line_gen, split_size, reducer_key=None):
    split_num = 0
    bytes_in_split = 0

    last_key_value = None

    for line in line_gen:
        same_key = False

        if reducer_key:
            key_value = reducer_key(line)
            same_key = (key_value == last_key_value)
            last_key_value = key_value

        if bytes_in_split >= split_size and not same_key:
            split_num += 1
            bytes_in_split = 0

        yield split_num, line
        bytes_in_split += len(line)
