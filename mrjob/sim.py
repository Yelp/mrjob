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
from os.path import dirname
from os.path import join
from os.path import relpath

from mrjob.cat import is_compressed
from mrjob.cat import open_input
from mrjob.compat import translate_jobconf_dict
from mrjob.conf import combine_local_envs
from mrjob.runner import MRJobRunner
from mrjob.util import unarchive

log = logging.getLogger(__name__)


class SimMRJobRunner(MRJobRunner):

    # keyword arguments that we ignore because they require real Hadoop.
    # We look directly at self._<kwarg_name> because they aren't in
    # self._opts
    _IGNORED_HADOOP_ATTRS = [
        '_hadoop_input_format',
        '_hadoop_output_format',
        '_partitioner',
        '_step_output_dir',
    ]

    # options that we ignore becaue they require real Hadoop.
    _IGNORED_HADOOP_OPTS = [
        'libjars',
    ]

    def __init__(self, **kwargs):
        super(SimMRJobRunner, self).__init__(**kwargs)

        # warn about ignored keyword arguments
        for ignored_attr in self._IGNORED_HADOOP_ATTRS:
            value = getattr(self, ignored_attr)
            if value is not None:
                log.warning(
                    'ignoring %s keyword arg (requires real Hadoop): %r' %
                    (ignored_attr[1:], value))

        for ignored_opt in self._IGNORED_HADOOP_OPTS:
            value = self._opts.get(ignored_opt)
            if value:  # ignore [], the default value of libjars
                log.warning(
                    'ignoring %s option (requires real Hadoop): %r' %
                    (ignored_opt, value))


    # re-implement these in your subclass

    def _invoke_task(self, task_type, step_num, input_path, output_path,
                     stderr_path, wd, env):
        """Run the given mapper/reducer, given the """
        NotImplementedError

    def _run_multiple(self, tasks, num_processes=None):
        """Run multiple tasks, possibly in parallel. Tasks are tuples of
        ``(func, args, kwargs)``.
        """
        raise NotImplementedError

    def _run(self):
        self._warn_ignored_opts()
        self._check_input_exists()
        self._create_setup_wrapper_script(local=True)

        last_output_paths = self._input_paths

        # run mapper, combiner, sort, reducer for each step
        for step_num, step in enumerate(self._get_steps()):
            log.info('Running step %d of %d...' % (
                step_num + 1, self._num_steps()))

            self._create_dist_cache_dir(step_num)
            os.mkdir(self._step_output_dir())

            map_splits = self._split_mapper_input(last_output_paths, step_num)

            self._run_mappers_and_combiners(map_splits, step_num)

            if 'reducer' in step:
                num_reducer_tasks = self._split_reducer_input(
                    step_num, len(map_splits))

                self._run_reducers(step_num, num_reducer_tasks)

    def _run_task(self, task_type, step_num, task_num, map_split=None):
        """Run one mapper, reducer, or combiner.

        This sets up everything the task needs to run, then passes it off to
        :py:meth:`_invoke_task`.
        """
        input_path = self._task_input_path(task_type, step_num, task_num)
        stderr_path = self._task_stderr_path(task_type, step_num, task_num)
        output_path = self._task_output_path(task_type, step_num, task_num)
        wd = self._setup_working_dir(task_type, step_num, task_num)
        env = self._env_for_task(task_type, step_num, task_num, map_split)

        self._launch_task(
            task_type, step_num, input_path, output_path, stderr_path, wd, env)

    def _run_mappers_and_combiners(self, map_splits, step_num):
        # TODO: possibly catch step failure
        self._run_multiple(
            (self._run_mapper_and_combiner, (map_split, step_num, task_num))
            for task_num, map_split in enumerate(map_splits)
        )

    def _run_mapper_and_combiner(self, map_split, step_num, task_num):
        step = self._get_step(step_num)

        if 'mapper' in step:
            self._run_task('mapper', step_num, task_num, map_split)
        else:
            # if no mapper, just pass the data through (see #1141)
            _symlink_or_copy(
                self._task_output_path('mapper', step_num, task_num),
                self._task_input_path('mapper', step_num, task_num))

        if 'combiner' in step:
            self._sort_combiner_input(step_num, task_num)
            self._run_task('combiner', step_num, task_num)

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
        cache_dir = self._dist_cache_dir(step_num)
        os.mkdir(cache_dir)

        for name, path in self._working_dir_mgr.name_to_path('file').items():

            dest = self._path_in_dist_cache_dir(name, step_num)
            shutil.copy(path, dest)
            _chmod_u_rx(dest)

        for name, path in self._working_dir_mgr.name_to_path(
                'archive').items():

            dest = self._path_in_dist_cache_dir(name, step_num)
            unarchive(path, dest)
            _chmod_u_rx(dest, recursive=True)

    def _env_for_task(self, task_type, step_num, task_num, map_split=None):
        """Set up environment variables for a subprocess (mapper, etc.)

        This combines, in decreasing order of priority:

        * environment variables set by the **cmdenv** option
        * **jobconf** environment variables set by our job (e.g.
          ``mapreduce.task.ismap`)
        * environment variables from **jobconf** options, translated to
          whatever version of Hadoop we're emulating
        * the current environment
        * PYTHONPATH set to current working directory

        We use :py:func:`~mrjob.conf.combine_local_envs`, so ``PATH``
        environment variables are handled specially.
        """
        user_jobconf = self._jobconf_for_step(step_num)

        simulated_jobconf = self._simulate_jobconf_for_step(
            step_num, task_type, task_num, map_split)

        def to_env(jobconf):
            return dict((k.replace('.', '_'), str(v))
                        for k, v in jobconf.items())

        # keep the current environment because we need PATH to find binaries
        # and make PYTHONPATH work
        return combine_local_envs(os.environ,
                                  to_env(user_jobconf),
                                  to_env(simulated_jobconf),
                                  self._opts['cmdenv'])

    def _simulate_jobconf_for_step(
            self, task_type, step_num, task_num, map_split=None):
        j = {}

        # TODO: these are really poor imtations of Hadoop keys. See #1254
        j['mapreduce.job.id'] = self._job_key
        j['mapreduce.task.id'] = 'task_%s_%s_%04d%d' % (
            self._job_key, task_type.lower(), step_num, task_num)
        j['mapreduce.task.attempt.id'] = 'attempt_%s_%s_%04d%d_0' % (
            self._job_key, task_type.lower(), step_num, task_num)

        j['mapreduce.task.ismap'] = (task_type == 'mapper')

        # TODO: is this the correct format?
        j['mapreduce.task.partition'] = str(task_num)

        j['mapreduce.task.output.dir'] = self._step_output_dir(step_num)

        working_dir = self._task_working_dir(task_type, step_num, task_num)
        j['mapreduce.job.local.dir'] = working_dir

        for x in ('archive', 'file'):
            named_paths = sorted(self._working_dir_mgr.name_to_path(x).items())

            # mapreduce.job.cache.archives
            # mapreduce.job.cache.files
            j['mapreduce.job.cache.%ss' % x] = ','.join(
                '%s#%s' % (path, name) for name, path in named_paths)

            # mapreduce.job.cache.local.archives
            # mapreduce.job.cache.local.files
            j['mapreduce.job.cache.local%ss' % x] = ','.join(
                join(working_dir, name) for name, path in named_paths)

        if map_split:
            # mapreduce.map.input.file
            # mapreduce.map.input.start
            # mapreduce.map.input.length
            for key, value in map_split:
                j['mapreduce.map.input.' + key] = str(value)

        # translate to correct Hadoop version
        return translate_jobconf_dict(j, self.get_hadoop_version(), warn=False)

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

        results = []

        for path in input_paths:
            with open_input(path) as src:
                if is_compressed(path):
                    # if file is compressed, uncompress it into a single split

                    # Hadoop tracks the compressed file's size
                    size = os.stat(path)[stat.ST_SIZE]

                    with split_fileobj_gen.next() as dest:
                        shutil.copyfileobj(src, dest)

                    results.append(dict(
                        file=path,
                        start=0,
                        length=size,
                    ))
                else:
                    # otherwise, split into one or more input files
                    start = 0
                    length = 0

                    for lines in _split_records(src, split_size):
                        with split_fileobj_gen.next() as dest:
                            for line in lines:
                                dest.write(line)
                                length += len(line)

                        results.append(dict(
                            file=path,
                            start=start,
                            length=length,
                        ))

                        start += length
                        length = 0

        return results

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

        num_reducer_tasks = 0

        with open(path, 'rb') as src:
            for lines in _split_records(src, split_size, reducer_key):
                with split_fileobj_gen.next() as dest:
                    shutil.copyfileobj(src, dest)
                    num_reducer_tasks += 1

        return num_reducer_tasks

    def _yield_split_fileobjs(self, task_type, step_num):
        """Used to split input for the given mapper/reducer.

        Yields writeable fileobjs for input splits (check their *name*
        attribute to get the path)
        """
        for task_num in itertools.count():
            path = self._task_input_path(task_type, step_num, task_num)
            self.fs.mkdir(dirname(path))
            yield open(path, 'rb')

    def _setup_working_dir(self, task_type, step_num, task_num):
        wd = self._task_working_dir(task_type, step_num, task_num)

        for type in ('archive', 'file'):
            for name, path in (
                    self._working_dir_mgr.name_to_path(type).items()):
                _symlink_or_copy(
                    self._path_in_dist_cache_dir(name), join(wd, name))

    def _last_task_type_in_step(self, step_num):
        step = self._get_step(step_num)

        if step.get('reducer'):
            return 'reducer'
        elif step.get('combiner'):
            return 'combiner'
        else:
            return 'mapper'

    # directory structure

    # cache/

    def _dist_cache_dir(self, step_num):
        return join(self._step_dir(step_num), 'cache')

    # cache/<name>

    def _path_in_dist_cache_dir(self, name, step_num):
        return join(self._dist_cache_dir(step_num), name)

    # step/<step_num>/

    def _step_dir(self, step_num):
        return join(self.tmp_dir, 'step', '%3d' % step_num)

    def _step_output_dir(self, step_num):
        if step_num == self._num_steps() - 1:
            return self._output_dir
        else:
            return self._intermediate_output_uri(step_num, local=True)

    # step/<step_num>/<task_type>/<task_num>/

    def _task_dir(self, task_type, step_num, task_num):
        return join(self._step_dir(step_num), task_type, '%5d' % task_num)

    def _task_input_path(self, task_type, step_num, task_num):
        return join(
            self._task_dir(task_type, step_num, task_num), 'input')

    def _task_stderr_path(self, task_type, step_num, task_num):
        return join(
            self._task_dir(task_type, step_num, task_num), 'stderr')

    def _task_output_path(self, task_type, step_num, task_num):
        """Where to output data for the given task.

        Usually this is just a file named "output" in the task's directory,
        but if it's the last task type in the step (usually the reducer),
        it outputs directly to part-XXXXX files in the step's output directory.
        """
        if task_type == self._last_task_type_in_step(step_num):
            return join(
                self._step_output_dir(step_num), 'part-%5d' % task_num)
        else:
            return join(
                self._task_dir(task_type, step_num, task_num), 'output')

    # step/<step_num>/<task_type>/<task_num>/wd

    def _task_working_dir(self, task_type, step_num, task_num):
        return join(self._task_dir(task_type, step_num, task_num), 'wd')

    # need this specifically since there's a global sort of reducer input

    def _sorted_reducer_input_path(self, step_num):
        return join(self._step_dir(step_num), 'reducer', 'sorted-input')

    def _sort_input(self, input_paths, output_path):
        """Sort lines from one or more input paths into a new file
        at *output_path*.

        By default this sorts in memory, but you can override this to
        use the :command:`sort` binary, etc.
        """
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















def _chmod_u_rx(path, recursive=False):
    # TODO: implement this
    pass


def _symlink_or_copy(path, dest):
    """Symlink from *dest* to *path*, using relative paths if possible.

    If symlinks aren't available, copy path to dest instead.
    """
    if hasattr(os, 'symlink'):
        log.debug('creating symlink %s <- %s' % (path, dest))
        os.symlink(relpath(path, dirname(dest)), dest)
    else:
        # TODO: use shutil.copy2() or shutil.copytree()
        raise NotImplementedError




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
