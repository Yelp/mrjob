# Copyright 2009-2012 Yelp and Contributors
# Copyright 2013 David Marin
# Copyright 2014-2016 Yelp and Contributors
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
import codecs
import logging
import os
import sys
import time
from io import BytesIO
from argparse import ArgumentParser
from argparse import ArgumentError

from mrjob.conf import combine_dicts
from mrjob.options import _add_basic_args
from mrjob.options import _add_job_args
from mrjob.options import _add_runner_args
from mrjob.options import _optparse_kwargs_to_argparse
from mrjob.options import _parse_raw_args
from mrjob.options import _print_help_for_runner
from mrjob.options import _print_basic_help
from mrjob.setup import parse_legacy_hash_path
from mrjob.step import StepFailedException
from mrjob.util import log_to_null
from mrjob.util import log_to_stream


log = logging.getLogger(__name__)


# sentinel value; used when running MRJob as a script
_READ_ARGS_FROM_SYS_ARGV = '_READ_ARGS_FROM_SYS_ARGV'


def _im_func(f):
    """Wrapper to get at the underlying function belonging to a method.

    Python 2 is slightly different because classes have "unbound methods"
    which wrap the underlying function, whereas on Python 3 they're just
    functions. (Methods work the same way on both versions.)
    """
    # "im_func" is the old Python 2 name for __func__
    if hasattr(f, '__func__'):
        return f.__func__
    else:
        return f


class MRJobLauncher(object):
    """Handle running a MapReduce job on an executable from the command line.
    This class will eventually support running arbitrary executables; for now
    it only supports :py:class:`~mrjob.job.MRJob` subclasses. Up to v0.5 it is
    effectively part of the :py:class:`~mrjob.job.MRJob` class itself and
    should not be used externally in any way.
    """
    # only MRJobLauncher expects the first argument to be script_path
    _FIRST_ARG_IS_SCRIPT_PATH = True

    def __init__(self, script_path=None, args=None, from_cl=False):
        """
        :param script_path: Path to script unless it's the first item of *args*
        :param args: Command line arguments
        :param from_cl: If not using sys.argv but still comming from the
                        command line (as opposed to a script, e.g. from
                        mrjob.cmd), don't override the option parser error
                        function (exit instead of throwing ValueError).
        """
        if script_path is not None:
            script_path = os.path.abspath(script_path)
        self._script_path = script_path

        # make sure we respect the $TZ (time zone) environment variable
        if hasattr(time, 'tzset'):
            time.tzset()

        # argument dests for args to pass through
        self._passthru_arg_dests = set()
        self._file_arg_dests = set()

        # there is no equivalent in argparse
        # remove this in v0.7.0
        if hasattr(self, 'OPTION_CLASS'):
            log.warning('OPTION_CLASS attribute is ignored; '
                        'mrjob now uses argparse instead of optparse')

        self.arg_parser = ArgumentParser(usage=self._usage(),
                                         add_help=False)
        self.configure_args()

        if (_im_func(self.configure_options) !=
                _im_func(MRJobLauncher.configure_options)):
            log.warning('configure_options() is deprecated and will be'
                        ' removed in v0.7.0; please use configure_args()'
                        ' instead.')
            self.configure_options()

        # don't pass None to parse_args unless we're actually running
        # the MRJob script
        if args is _READ_ARGS_FROM_SYS_ARGV:
            self._cl_args = sys.argv[1:]
        else:
            # don't pass sys.argv to self.arg_parser, and have it
            # raise an exception on error rather than printing to stderr
            # and exiting.
            self._cl_args = args or []

            def error(msg):
                raise ValueError(msg)

            if not from_cl:
                self.arg_parser.error = error

        self.load_args(self._cl_args)

        if (_im_func(self.load_options) !=
                _im_func(MRJobLauncher.load_options)):
            log.warning('load_options() is deprecated and will be'
                        ' removed in v0.7.0; please use load_args()'
                        ' instead.')
            self.load_options(self._cl_args)

        # Make it possible to redirect stdin, stdout, and stderr, for testing
        # See sandbox(), below.
        #
        # These should always read/write bytes, not unicode. Generally,
        # on Python 2, sys.std* can read and write bytes, whereas on Python 3,
        # you need to use sys.std*.buffer (which doesn't exist on Python 2).
        #
        # However, certain Python 3 environments, such as Jupyter notebook,
        # act more like Python 2. See #1441.
        self.stdin = getattr(sys.stdin, 'buffer', sys.stdin)
        self.stdout = getattr(sys.stdout, 'buffer', sys.stdout)
        self.stderr = getattr(sys.stderr, 'buffer', sys.stderr)

    @classmethod
    def _usage(cls):
        """Command line usage string for this class"""
        return ("usage: mrjob run [script path|executable path|--help]"
                " [options] [input files]")

    def _print_help(self, options):
        """Print help for this job. This will either print runner
        or basic help. Override to allow other kinds of help."""
        if options.runner:
            _print_help_for_runner(
                self._runner_opt_names(), options.deprecated)
        else:
            _print_basic_help(self.arg_parser,
                              self._usage(),
                              options.deprecated)

    @classmethod
    def run(cls, args=_READ_ARGS_FROM_SYS_ARGV):
        """Entry point for running job from the command-line.

        This is also the entry point when a mapper or reducer is run
        by Hadoop Streaming.

        Does one of:

        * Print step information (:option:`--steps`). See :py:meth:`show_steps`
        * Run a mapper (:option:`--mapper`). See :py:meth:`run_mapper`
        * Run a combiner (:option:`--combiner`). See :py:meth:`run_combiner`
        * Run a reducer (:option:`--reducer`). See :py:meth:`run_reducer`
        * Run the entire job. See :py:meth:`run_job`
        """
        # load options from the command line
        launcher = cls(args=args)
        launcher.run_job()

    def execute(self):
        # Launcher only runs jobs, doesn't do any Hadoop Streaming stuff
        self.run_job()

    def make_runner(self):
        """Make a runner based on command-line arguments, so we can
        launch this job on EMR, on Hadoop, or locally.

        :rtype: :py:class:`mrjob.runner.MRJobRunner`
        """
        return self._runner_class()(**self._runner_kwargs())

    @classmethod
    def set_up_logging(cls, quiet=False, verbose=False, stream=None):
        """Set up logging when running from the command line. This is also
        used by the various command-line utilities.

        :param bool quiet: If true, don't log. Overrides *verbose*.
        :param bool verbose: If true, set log level to ``DEBUG`` (default is
                             ``INFO``)
        :param bool stream: Stream to log to (default is ``sys.stderr``)

        This will also set up a null log handler for boto3, so we don't get
        warnings if boto3 tries to log about throttling and whatnot.
        """
        if quiet:
            log_to_null(name='mrjob')
            log_to_null(name='__main__')
        else:
            log_to_stream(name='mrjob', debug=verbose, stream=stream)
            log_to_stream(name='__main__', debug=verbose, stream=stream)

    def run_job(self):
        """Run the all steps of the job, logging errors (and debugging output
        if :option:`--verbose` is specified) to STDERR and streaming the
        output to STDOUT.

        Called from :py:meth:`run`. You'd probably only want to call this
        directly from automated tests.
        """
        # self.stderr is strictly binary, need to wrap it so it's possible
        # to log to it in Python 3
        log_stream = codecs.getwriter('utf_8')(self.stderr)

        self.set_up_logging(quiet=self.options.quiet,
                            verbose=self.options.verbose,
                            stream=log_stream)

        with self.make_runner() as runner:
            try:
                runner.run()
            except StepFailedException as e:
                # no need for a runner stacktrace if step failed; runners will
                # log more useful information anyway
                log.error(str(e))
                sys.exit(1)

            if not self.options.no_output:
                for chunk in runner.cat_output():
                    self.stdout.write(chunk)
                self.stdout.flush()

    ### Command-line arguments ###

    def configure_args(self):
        """Define arguments for this script. Called from :py:meth:`__init__()`.

        Re-define to define custom command-line arguments or pass
        through existing ones::

            def configure_args(self):
                super(MRYourJob, self).configure_args()

                self.add_passthru_arg(...)
                self.add_file_arg(...)
                self.pass_arg_through(...)
                ...
        """
        self.arg_parser.add_argument(
            '-h', '--help', dest='help', action='store_true',
            help='show this message and exit')

        self.arg_parser.add_argument(
            '--deprecated', dest='deprecated', action='store_true',
            help='include help for deprecated options')

        # if script path isn't set, expect it on the command line
        if self._FIRST_ARG_IS_SCRIPT_PATH:
            self.arg_parser.add_argument(
                dest='script_path',
                help='path of script to launch')

        self.arg_parser.add_argument(
            dest='args', nargs='*',
            help=('input paths to read (or stdin if not set). If --spark'
                  ' is set, the input and output path for the spark job.'))

        _add_basic_args(self.arg_parser)
        _add_job_args(self.arg_parser)
        _add_runner_args(self.arg_parser)

    def load_args(self, args):
        """Load command-line options into ``self.options`` and
        ``self._script_path``.

        Called from :py:meth:`__init__()` after :py:meth:`configure_args`.

        :type args: list of str
        :param args: a list of command line arguments. ``None`` will be
                     treated the same as ``[]``.

        Re-define if you want to post-process command-line arguments::

            def load_args(self, args):
                super(MRYourJob, self).load_args(args)

                self.stop_words = self.options.stop_words.split(',')
                ...
        """
        self.options = self.arg_parser.parse_args(args)

        if self.options.help:
            self._print_help(self.options)
            sys.exit(0)

        if self._FIRST_ARG_IS_SCRIPT_PATH:
            # should always be set, just hedging
            self._script_path = self.options.script_path

    def add_file_arg(self, *args, **kwargs):
        """Add a command-line option that sends an external file
        (e.g. a SQLite DB) to Hadoop::

             def configure_args(self):
                super(MRYourJob, self).configure_args()
                self.add_file_arg('--scoring-db', help=...)

        This does the right thing: the file will be uploaded to the working
        dir of the script on Hadoop, and the script will be passed the same
        option, but with the local name of the file in the script's working
        directory.

        We suggest against sending Berkeley DBs to your job, as
        Berkeley DB is not forwards-compatible (so a Berkeley DB that you
        construct on your computer may not be readable from within
        Hadoop). Use SQLite databases instead. If all you need is an on-disk
        hash table, try out the :py:mod:`sqlite3dbm` module.
        """
        if kwargs.get('type') not in (None, 'string'):
            raise ArgumentError(
                'file options must take strings')

        if kwargs.get('action') not in (None, 'append', 'store'):
            raise ArgumentError(
                "file options must use the actions 'store' or 'append'")

        pass_opt = self.arg_parser.add_argument(*args, **kwargs)

        self._file_arg_dests.add(pass_opt.dest)

    def add_passthru_arg(self, *args, **kwargs):
        """Function to create options which both the job runner
        and the job itself respect (we use this for protocols, for example).

        Use it like you would use
        :py:func:`argparse.ArgumentParser.add_argument`::

            def configure_args(self):
                super(MRYourJob, self).configure_args()
                self.add_passthru_arg(
                    '--max-ngram-size', type=int, default=4, help='...')

        If you want to pass files through to the mapper/reducer, use
        :py:meth:`add_file_arg` instead.

        If you want to pass through a built-in option (e.g. ``--runner``, use
        :py:meth:`pass_arg_through` instead.
        """
        pass_opt = self.arg_parser.add_argument(*args, **kwargs)

        self._passthru_arg_dests.add(pass_opt.dest)

    def pass_arg_through(self, opt_str):
        """Pass the given argument through to the job."""

        # _actions is hidden but the interface appears to be stable,
        # and there's no non-hidden interface we can use
        for action in self.arg_parser._actions:
            if opt_str in action.option_strings or opt_str == action.dest:
                self._passthru_arg_dests.add(action.dest)
                break
        else:
            raise ValueError('unknown arg: %s', opt_str)

    def is_task(self):
        """True if this is a mapper, combiner, or reducer.

        This is mostly useful inside :py:meth:`load_args`, to disable
        loading args when we aren't running inside Hadoop Streaming.
        """
        return False

    ### old optparse shims ###

    @property
    def args(self):
        class_name = self.__class__.__name__
        log.warning(
            '%s.args is a deprecated alias for %s.options.args, and will'
            ' be removed in v0.7.0' % (class_name, class_name))
        return self.options.args

    def configure_options(self):
        """.. deprecated:: 0.6.0

        Use `:py:meth:`configure_args` instead.
        """
        pass  # deprecation warning is in __init__()

    def load_options(self, args):
        """.. deprecated:: 0.6.0

        Use `:py:meth:`load_args` instead.
        """
        pass  # deprecation warning is in __init__()

    def add_file_option(self, *args, **kwargs):
        """.. deprecated:: 0.6.0

        Like :py:meth:`add_file_arg` except that it emulates the
        old :py:mod:`optparse` interface (which is almost identical).
        """
        log.warning(
            'add_file_option() is deprecated and will be removed in'
            ' v0.7.0. Use add_file_arg() instead.')

        self.add_file_arg(*args, **_optparse_kwargs_to_argparse(**kwargs))

    def add_passthrough_option(self, *args, **kwargs):
        """.. deprecated:: 0.6.0

        Like :py:meth:`add_passthru_arg` except that it emulates the
        old :py:mod:`optparse` interface (which is almost identical).
        """
        log.warning(
            'add_passthrough_option() is deprecated and will be removed in'
            ' v0.7.0. Use add_passthru_arg() instead.')

        self.add_passthru_arg(*args, **_optparse_kwargs_to_argparse(**kwargs))

    def pass_through_option(self, opt_str):
        """.. deprecated:: 0.6.0

        Like :py:meth:`pass_arg_througj` except that it emulates the
        old :py:mod:`optparse` interface (which is almost identical).
        """
        log.warning(
            'pass_through_option() is deprecated and will be removed in'
            ' v0.7.0. Use pass_arg_through() instead.')

        self.pass_arg_through(opt_str)

    ### runners ###

    def _runner_class(self):
        """Runner class, as indicated by ``--runner``. This uses conditional
        imports to avoid importing runner modules that we don't need (and may
        not have libraries for).

        Defaults to ``'local'`` and disallows use of inline runner.
        """
        if self.options.runner == 'dataproc':
            from mrjob.dataproc import DataprocJobRunner
            return DataprocJobRunner

        elif self.options.runner == 'emr':
            from mrjob.emr import EMRJobRunner
            return EMRJobRunner

        elif self.options.runner == 'hadoop':
            from mrjob.hadoop import HadoopJobRunner
            return HadoopJobRunner

        elif self.options.runner == 'inline':
            raise ValueError("inline is not supported in the multi-lingual"
                             " launcher.")

        else:
            # run locally by default
            from mrjob.local import LocalMRJobRunner
            return LocalMRJobRunner

    def _runner_kwargs(self):
        return combine_dicts(
            self._non_option_kwargs(),
            self._kwargs_from_switches(self._runner_opt_names()),
            self._job_kwargs(),
        )

    def _runner_opt_names(self):
        return self._runner_class().OPT_NAMES

    def _non_option_kwargs(self):
        """Keyword arguments to runner constructor that can't be set
        in mrjob.conf.

        These should match the (named) arguments to
        :py:meth:`~mrjob.runner.MRJobRunner.__init__`.
        """
        # build extra_args
        raw_args = _parse_raw_args(self.arg_parser, self._cl_args)

        extra_args = []

        for dest, option_string, args in raw_args:
            if dest in self._file_arg_dests:
                extra_args.append(option_string)
                extra_args.append(parse_legacy_hash_path('file', args[0]))
            elif dest in self._passthru_arg_dests:
                # special case for --hadoop-arg=-verbose etc.
                if (option_string and len(args) == 1 and
                        args[0].startswith('-')):
                    extra_args.append('%s=%s' % (option_string, args[0]))
                else:
                    if option_string:
                        extra_args.append(option_string)
                    extra_args.extend(args)

        return dict(
            conf_paths=self.options.conf_paths,
            extra_args=extra_args,
            hadoop_input_format=self.hadoop_input_format(),
            hadoop_output_format=self.hadoop_output_format(),
            input_paths=self.options.args,
            mr_job_script=self._script_path,
            output_dir=self.options.output_dir,
            partitioner=self.partitioner(),
            stdin=self.stdin,
            step_output_dir=self.options.step_output_dir,
        )

    def _kwargs_from_switches(self, keys):
        return dict(
            (key, getattr(self.options, key))
            for key in keys if hasattr(self.options, key)
        )

    def _job_kwargs(self):
        """Keyword arguments to the runner class that can be specified
        by the job/launcher itself."""
        return dict(
            jobconf=self.jobconf(),
            libjars=self.libjars(),
            partitioner=self.partitioner(),
            sort_values=self.sort_values(),
        )

    ### Hooks for options defined by the job ###

    def hadoop_input_format(self):
        """See :py:meth:`mrjob.job.MRJob.hadoop_input_format`."""
        return None

    def hadoop_output_format(self):
        """See :py:meth:`mrjob.job.MRJob.hadoop_output_format`."""
        return None

    def jobconf(self):
        """See :py:meth:`mrjob.job.MRJob.jobconf`."""
        return {}

    def libjars(self):
        """See :py:meth:`mrjob.job.MRJob.libjars`."""
        return []

    def partitioner(self):
        """See :py:meth:`mrjob.job.MRJob.partitioner`."""
        return None

    def sort_values(self):
        """See :py:meth:`mrjob.job.MRJob.sort_values`."""
        return None

    ### Testing ###

    def sandbox(self, stdin=None, stdout=None, stderr=None):
        """Redirect stdin, stdout, and stderr for automated testing.

        You can set stdin, stdout, and stderr to file objects. By
        default, they'll be set to empty ``BytesIO`` objects.
        You can then access the job's file handles through ``self.stdin``,
        ``self.stdout``, and ``self.stderr``. See :ref:`testing` for more
        information about testing.

        You may call sandbox multiple times (this will essentially clear
        the file handles).

        ``stdin`` is empty by default. You can set it to anything that yields
        lines::

            mr_job.sandbox(stdin=BytesIO(b'some_data\\n'))

        or, equivalently::

            mr_job.sandbox(stdin=[b'some_data\\n'])

        For convenience, this sandbox() returns self, so you can do::

            mr_job = MRJobClassToTest().sandbox()

        Simple testing example::

            mr_job = MRYourJob.sandbox()
            self.assertEqual(list(mr_job.reducer('foo', ['a', 'b'])), [...])

        More complex testing example::

            from BytesIO import BytesIO

            from mrjob.parse import parse_mr_job_stderr
            from mrjob.protocol import JSONProtocol

            mr_job = MRYourJob(args=[...])

            fake_input = '"foo"\\t"bar"\\n"foo"\\t"baz"\\n'
            mr_job.sandbox(stdin=BytesIO(fake_input))

            mr_job.run_reducer(link_num=0)

            self.assertEqual(mrjob.stdout.getvalue(), ...)
            self.assertEqual(parse_mr_job_stderr(mr_job.stderr), ...)
        """
        self.stdin = stdin or BytesIO()
        self.stdout = stdout or BytesIO()
        self.stderr = stderr or BytesIO()

        return self


if __name__ == '__main__':
    MRJobLauncher.run()
