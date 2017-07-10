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
from mrjob.logs.counters import _format_counters
from mrjob.parse import parse_mr_job_stderr
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
    ]

    # options that we ignore becaue they require real Hadoop.
    _IGNORED_HADOOP_OPTS = [
        'libjars',
    ]

    def __init__(self, **kwargs):
        super(SimMRJobRunner, self).__init__(**kwargs)

        self._counters = []

        # warn about ignored keyword arguments
        # TODO: no need to look at attrs, just look at kwargs

        for ignored_attr in self._IGNORED_HADOOP_ATTRS:
            value = getattr(self, ignored_attr)
            if value is not None:
                log.warning(
                    'ignoring %s keyword arg (requires real Hadoop): %r' %
                    (ignored_attr[1:], value))

        # TODO: libjars should just not be an option for local runners

        for ignored_opt in self._IGNORED_HADOOP_OPTS:
            value = self._opts.get(ignored_opt)
            if value:  # ignore [], the default value of libjars
                log.warning(
                    'ignoring %s option (requires real Hadoop): %r' %
                    (ignored_opt, value))


    # re-implement these in your subclass

    def _invoke_task(self, task_type, step_num, task_num,
                     stdin, stdout, stderr, wd, env):
        """Run the given mapper/reducer, with the job's file handles
        working dir, and environment already set up."""
        NotImplementedError

    def _run_multiple(self, tasks, num_processes=None):
        """Run multiple tasks, possibly in parallel. Tasks are tuples of
        ``(func, args, kwargs)``. *func* must be pickleable; if you want to run
        instance methods, use :py:func:`_apply_method` to wrap them.
        """
        raise NotImplementedError

    def _log_cause_of_error(self, ex):
        """Log why the job failed."""
        pass

    def _run(self):
        self._check_input_exists()
        self._create_setup_wrapper_script(local=True)

        # run mapper, combiner, sort, reducer for each step
        for step_num, step in enumerate(self._get_steps()):
            log.info('Running step %d of %d...' % (
                step_num + 1, self._num_steps()))

            self._counters.append({})

            try:
                self._create_dist_cache_dir(step_num)
                self.fs.mkdir(self._output_dir_for_step(step_num))

                map_splits = self._split_mapper_input(
                    self._input_paths_for_step(step_num), step_num)

                self._run_mappers_and_combiners(step_num, map_splits)

                if 'reducer' in step:
                    self._sort_reducer_input(step_num, len(map_splits))
                    num_reducer_tasks = self._split_reducer_input(step_num)

                    self._run_reducers(step_num, num_reducer_tasks)

                self._log_counters(step_num)

            except Exception as ex:
                self._log_counters(step_num)
                self._log_cause_of_error(ex)

                raise

    def _run_task(self, task_type, step_num, task_num, map_split=None):
        """Run one mapper, reducer, or combiner.

        This sets up everything the task needs to run, then passes it off to
        :py:meth:`_invoke_task`.
        """
        log.debug('running step %d, %s %d' % (step_num, task_type, task_num))

        input_path = self._task_input_path(task_type, step_num, task_num)
        stderr_path = self._task_stderr_path(task_type, step_num, task_num)
        output_path = self._task_output_path(task_type, step_num, task_num)
        wd = self._setup_working_dir(task_type, step_num, task_num)
        env = self._env_for_task(task_type, step_num, task_num, map_split)

        with open(input_path, 'rb') as stdin, \
                open(output_path, 'wb') as stdout, \
                open(stderr_path, 'wb') as stderr:

            self._invoke_task(
                task_type, step_num, task_num, stdin, stdout, stderr, wd, env)

    def _run_mappers_and_combiners(self, step_num, map_splits):
        # TODO: possibly catch step failure
        try:
            self._run_multiple(
                (_apply_method,
                 (self, '_run_mapper_and_combiner',
                  step_num, task_num, map_split),
                 {})
                 for task_num, map_split in enumerate(map_splits)
            )
        finally:
            self._parse_task_counters('mapper', step_num)
            self._parse_task_counters('combiner', step_num)

    def _parse_task_counters(self, task_type, step_num):
        """Parse all stderr files from the given task (if any)."""
        stderr_paths = self.fs.ls(self._task_stderr_paths_glob(
            task_type, step_num))

        for stderr_path in stderr_paths:
            with open(stderr_path, 'rb') as stderr:
                parse_mr_job_stderr(stderr, counters=self._counters[step_num])

    def counters(self):
        return self._counters

    def _run_mapper_and_combiner(self, step_num, task_num, map_split):
        step = self._get_step(step_num)

        if 'mapper' in step:
            self._run_task('mapper', step_num, task_num, map_split)
        else:
            # if no mapper, just pass the data through (see #1141)
            _symlink_or_copy(
                self._task_input_path('mapper', step_num, task_num),
                self._task_output_path('mapper', step_num, task_num))

        if 'combiner' in step:
            self._sort_combiner_input(step_num, task_num)
            self._run_task('combiner', step_num, task_num)

    def _run_reducers(self, step_num, num_reducer_tasks):
        try:
            self._run_multiple(
                (_apply_method,
                 (self, '_run_task', 'reducer', step_num, task_num),
                 {})
                 for task_num in range(num_reducer_tasks)
            )
        finally:
            self._parse_task_counters('reducer', step_num)

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

        log.debug('creating simulated Distributed Cache dir: %s' % cache_dir)
        self.fs.mkdir(cache_dir)

        for name, path in self._working_dir_mgr.name_to_path('file').items():

            dest = self._path_in_dist_cache_dir(name, step_num)
            log.debug('copying %s -> %s' % (path, dest))
            shutil.copy(path, dest)
            _chmod_u_rx(dest)

        for name, path in self._working_dir_mgr.name_to_path(
                'archive').items():

            dest = self._path_in_dist_cache_dir(name, step_num)
            log.debug('unarchiving %s -> %s' % (path, dest))
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
            task_type, step_num, task_num, map_split)

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

        # TODO: these are really poor imtations of Hadoop IDs. See #1254
        j['mapreduce.job.id'] = self._job_key
        j['mapreduce.task.id'] = 'task_%s_%s_%04d%d' % (
            self._job_key, task_type.lower(), step_num, task_num)
        j['mapreduce.task.attempt.id'] = 'attempt_%s_%s_%04d%d_0' % (
            self._job_key, task_type.lower(), step_num, task_num)

        j['mapreduce.task.ismap'] = str(task_type == 'mapper').lower()

        # TODO: is this the correct format?
        j['mapreduce.task.partition'] = str(task_num)

        j['mapreduce.task.output.dir'] = self._output_dir_for_step(step_num)

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
            j['mapreduce.job.cache.local.%ss' % x] = ','.join(
                join(working_dir, name) for name, path in named_paths)

        if map_split:
            # mapreduce.map.input.file
            # mapreduce.map.input.start
            # mapreduce.map.input.length
            for key, value in map_split.items():
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

                    with next(split_fileobj_gen) as dest:
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
                        with next(split_fileobj_gen) as dest:
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

    def _split_reducer_input(self, step_num):
        """Split a single, uncompressed file containing sorted input for the
        reducer into input files for each reducer task.

        Yield the paths of the reducer input files."""
        path = self._sorted_reducer_input_path(step_num)

        log.debug('splitting reducer input: %s' % path)

        size = os.stat(path)[stat.ST_SIZE]
        split_size = size // (self._num_reducers(step_num) * 2)

        # yield output fileobjs as needed
        split_fileobj_gen = self._yield_split_fileobjs('reducer', step_num)

        def reducer_key(line):
            return line.split(b'\t')[0]

        num_reducer_tasks = 0

        with open(path, 'rb') as src:
            for records in _split_records(src, split_size, reducer_key):
                with next(split_fileobj_gen) as dest:
                    for record in records:
                        dest.write(record)
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
            yield open(path, 'wb')

    def _setup_working_dir(self, task_type, step_num, task_num):
        wd = self._task_working_dir(task_type, step_num, task_num)
        self.fs.mkdir(wd)

        for type in ('archive', 'file'):
            for name, path in (
                    self._working_dir_mgr.name_to_path(type).items()):
                _symlink_or_copy(
                    self._path_in_dist_cache_dir(name, step_num),
                    join(wd, name))

        return wd

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
        return join(self._get_local_tmp_dir(), 'step', '%03d' % step_num)

    def _input_paths_for_step(self, step_num):
        if step_num == 0:
            return [path for input_path_glob in self._get_input_paths()
                    for path in self.fs.ls(input_path_glob)]
        else:
            return self.fs.ls(
                join(self._output_dir_for_step(step_num - 1), 'part-*'))

    def _output_dir_for_step(self, step_num):
        if step_num == self._num_steps() - 1:
            if not self._output_dir:
                self._output_dir = join(self._get_local_tmp_dir(), 'output')
            return self._output_dir
        else:
            return self._intermediate_output_uri(step_num, local=True)

    def _default_step_output_dir(self):
        return join(self._get_local_tmp_dir(), 'step-output')

    # step/<step_num>/<task_type>/<task_num>/

    def _task_dir(self, task_type, step_num, task_num):
        return join(self._step_dir(step_num), task_type, '%05d' % task_num)

    def _task_input_path(self, task_type, step_num, task_num):
        return join(
            self._task_dir(task_type, step_num, task_num), 'input')

    def _task_stderr_path(self, task_type, step_num, task_num):
        return join(
            self._task_dir(task_type, step_num, task_num), 'stderr')

    def _task_stderr_paths_glob(self, task_type, step_num):
        return join(
            self._step_dir(step_num), task_type, '*', 'stderr')

    def _task_output_path(self, task_type, step_num, task_num):
        """Where to output data for the given task.

        Usually this is just a file named "output" in the task's directory,
        but if it's the last task type in the step (usually the reducer),
        it outputs directly to part-XXXXX files in the step's output directory.
        """
        if task_type == self._last_task_type_in_step(step_num):
            return join(
                self._output_dir_for_step(step_num), 'part-%05d' % task_num)
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
        log.debug('sorting in memory: %s -> %s' %
                  (', '.join(input_paths), output_path))
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

    def _sort_combiner_input(self, step_num, task_num):
        input_path = self._task_output_path('mapper', step_num, task_num)
        output_path = self._task_input_path('combiner', step_num, task_num)
        self.fs.mkdir(dirname(output_path))

        self._sort_input([input_path], output_path)

    def _sort_reducer_input(self, step_num, num_map_tasks):
        step = self._get_step(step_num)

        output_path = self._sorted_reducer_input_path(step_num)
        self.fs.mkdir(dirname(output_path))

        prev_task_type = 'combiner' if step.get('combiner') else 'mapper'
        input_paths = [
            self._task_output_path(prev_task_type, step_num, task_num)
            for task_num in range(num_map_tasks)
        ]

        self._sort_input(input_paths, output_path)

    def _log_counters(self, step_num):
        counters = self.counters()[step_num]
        if counters:
            log.info('\n%s\n' % _format_counters(counters))


def _chmod_u_rx(path, recursive=False):
    if recursive:
        for dirname, _, filenames in os.walk(path, followlinks=True):
            for filename in filenames:
                _chmod_u_rx(join(dirname, filename))
    else:
        if hasattr(os, 'chmod'):  # only available on Unix, Windows
            os.chmod(path, stat.S_IRUSR | stat.S_IXUSR)


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
    grouped_record_gen = _group_records_for_split(
        record_gen, split_size, reducer_key)

    for group_id, grouped_records in itertools.groupby(
            grouped_record_gen, key=lambda gr: gr[0]):
        yield (record for _, record in grouped_records)
    else:
        # special case for empty files
        yield ()


def _group_records_for_split(record_gen, split_size, reducer_key=None):
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


def _apply_method(self, method_name, *args, **kwargs):
    """Shim to turn method calls into pickleable function calls."""
    getattr(self, method_name)(*args, **kwargs)
