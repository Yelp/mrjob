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
import platform
import shutil
import stat
from multiprocessing import cpu_count
from os.path import join
from subprocess import CalledProcessError
from subprocess import check_call

from mrjob.cat import is_compressed
from mrjob.cat import open_input
from mrjob.options import _allowed_keys
from mrjob.options import _combiners
from mrjob.options import _deprecated_aliases
from mrjob.runner import RunnerOptionStore
from mrjob.util import cmd_line
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

        # should we fall back to sorting in memory?
        self._bad_sort_bin = False

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
        """Take one or more input paths (which may be compressed) and split
        it to create the input files for the map tasks.

        Yields "splits", which are dictionaries with the following keys:

        input: path of input for one mapper
        file: path of original file
        start, length: chunk of original file in *input*

        Uncompressed files will not be split (even ``.bz2`` files);
        uncompressed files will be split as to to attempt to create
        twice as many input files as there are mappers.
        """
        input_paths = list(input_paths)

        # determine split size
        split_size = self._pick_mapper_split_size(input_paths, step_num)

        # yield output fileobjs as needed
        split_fileobj_gen = self._yield_split_fileobjs('mapper', step_num)

        for path in input_paths:
            with open_input(path) as src:
                if is_compressed(path):
                    # if file is compressed, uncompress it into a single split

                    # Hadoop tracks the compressed file's size
                    size = os.stat(path)[stat.ST_SIZE]

                    with split_fileobj_gen.next() as dest:
                        shutil.copyfileobj(src, dest)

                        yield dict(
                            input=dest.name,
                            file=path,
                            start=0,
                            length=size)
                else:
                    # otherwise, split into one or more input files
                    start = 0
                    length = 0

                    for lines in _split_records(src, split_size):
                        with split_fileobj_gen.next() as dest:
                            for line in lines:
                                dest.write(line)
                                length += len(line)

                                yield dict(
                                    input=dest.name,
                                    file=path,
                                    start=start,
                                    length=length)

                        start += length
                        length = 0

    def _pick_mapper_split_size(self, input_paths, step_num):
        if not isinstance(input_paths, list):
            raise TypeError

        target_num_splits = self._num_mappers(step_num) * 2

        # decide on a split size to approximate target_num_splits
        num_compressed = 0
        uncompressed_bytes = 0

        for path in input_paths:
            if path.endswith('.gz') or path.endswith('.bz'):
                num_compressed += 1
            else:
                uncompressed_bytes += os.stat(path)[stat.ST_SIZE]

        return uncompressed_bytes // max(
            target_num_splits - num_compressed, 1)

    def _split_reducer_input(self, path, step_num):
        """Split a single, uncompressed file containing sorted input for the
        reducer into input files for each reducer task.

        Yield the paths of the reducer input files."""
        size = os.stat(path)[stat.ST_SIZE]
        split_size = size // (self._num_reducers(step_num) * 2)

        # yield output fileobjs as needed
        split_fileobj_gen = self._yield_split_fileobjs('reducer', step_num)

        def reducer_key(line):
            return line.split(b'\t')[0]

        with open(path, 'rb') as src:
            for lines in _split_records(src, split_size, reducer_key):
                with split_fileobj_gen.next() as dest:
                    shutil.copyfileobj(src, dest)
                    yield dest.name

    def _yield_split_fileobjs(self, task_type, step_num):
        """Used to split input for the given mapper/reducer.

        Yields writeable fileobjs for input splits (check their *name*
        attribute to get the path)
        """
        for task_num in itertools.count():
            task_dir = self._task_dir_path('mapper', step_num, task_num)
            self.fs.mkdir(task_dir)
            return open(join(task_dir, 'input'), 'rb')

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

    def _sort_input(self, input_paths, output_path):
        """Sort lines from one or more input paths into a new file
        at *output_path*.

        This uses a unix sort binary, windows sort binary, or Python
        as appropriate.
        """
        if not input_paths:
            raise ValueError('Must specify at least one input path.')

        if not (self._bad_sort_bin or platform.system() == 'Windows'):
            env = os.environ.copy()

            # ignore locale when sorting
            env['LC_ALL'] = 'C'

            # Make sure that the tmp dir environment variables are changed if
            # the default is changed.
            env['TMP'] = self._opts['local_tmp_dir']
            env['TMPDIR'] = self._opts['local_tmp_dir']

            err_path = os.path.join(self._get_local_tmp_dir(), 'sort-stderr')

            with open(output_path, 'wb') as output:
                with open(err_path, 'wb') as err:
                    args = self._sort_bin() + list(input_paths)
                    log.debug('> %s' % cmd_line(args))
                    try:
                        check_call(args, stdout=output, stderr=err,
                                   env=self._sort_env())
                        return
                    except CalledProcessError:
                        log.error(
                            '`%s` failed, falling back to in-memory sort' %
                            cmd_line(self._sort_bin()))
                        with open(err_path) as err:
                            for line in err:
                                log.error('STDERR: %s' % line.rstrip('\r\n'))
                    except OSError:
                        log.error(
                            'no sort binary, falling back to in-memory sort')

        self._bad_sort_bin = True

        # sort in memory
        log.debug('sorting in memory: %s' % ', '.join(input_paths))
        lines = []

        for input_path in input_paths:
            with open(input_path, 'rb') as input:
                lines.extend(input)

        if self._sort_values:
            lines.sort()
        else:
            lines.sort(key=lambda line: line.split('\t')[0])

        with open(output_path, 'wb') as output:
            for line in lines:
                output.write(line)

    def _sort_bin(self):
        """The binary to use to sort input.

        (On Windows, we go straight to sorting in memory.)
        """
        if self._opts['sort_bin']:
            return self._opts['sort_bin']
        elif self._sort_values:
            return ['sort']
        else:
            # only sort on the reducer key (see #660)
            return ['sort', '-t', '\t', '-k', '1,1', '-s']












def _chmod_u_rx(path, recursive=False):
    # TODO: implement this
    pass



def _split_records(record_gen, split_size, reducer_key=None):
    """Given a stream of records (bytestrings, usually lines), yield groups of
    records (as generators such that the total number of bytes in each group
    only barely exceeds *split_size*, and, if *reducer_key* is set, consecutive
    records with the same key will be in the same split."""
    labeled_record_gen = _label_records_for_split(
        record_gen, split_size, reducer_key)

    for label, labeled_records in itertools.groupby(
            labeled_record_gen, key=lambda lr: lr[0]):
        yield (record for _, record in labeled_records)


def _label_records_for_split(record_gen, split_size, reducer_key=None):
    """Helper for _split_records()."""
    split_num = 0
    bytes_in_split = 0

    last_key_value = None

    for record in record_gen:
        same_key = False

        if reducer_key:
            key_value = reducer_key(record)
            same_key = (key_value == last_key_value)
            last_key_value = key_value

        if bytes_in_split >= split_size and not same_key:
            split_num += 1
            bytes_in_split = 0

        yield split_num, record
        bytes_in_split += len(record)
