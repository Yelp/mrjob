# Copyright 2009-2012 Yelp and Contributors
# Copyright 2013 David Marin
# Copyright 2014-2016 Yelp and Contributors
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
from optparse import Option
from optparse import OptionError
from optparse import OptionGroup
from optparse import OptionParser

from mrjob.conf import combine_dicts
from mrjob.options import _add_basic_opts
from mrjob.options import _add_dataproc_emr_opts
from mrjob.options import _add_dataproc_opts
from mrjob.options import _add_emr_opts
from mrjob.options import _add_hadoop_emr_opts
from mrjob.options import _add_hadoop_opts
from mrjob.options import _add_local_opts
from mrjob.options import _add_protocol_opts
from mrjob.options import _add_runner_opts
from mrjob.options import _alphabetize_options
from mrjob.options import _fix_custom_options
from mrjob.options import _print_help_for_groups
from mrjob.py2 import PY2
from mrjob.step import StepFailedException
from mrjob.util import log_to_null
from mrjob.util import log_to_stream
from mrjob.util import parse_and_save_options


log = logging.getLogger(__name__)


# sentinel value; used when running MRJob as a script
_READ_ARGS_FROM_SYS_ARGV = '_READ_ARGS_FROM_SYS_ARGV'

# options only used to modify other options; don't pass to runners
_FAKE_OPTIONS = set([
    'no_emr_api_params',
])


class MRJobLauncher(object):
    """Handle running a MapReduce job on an executable from the command line.
    This class will eventually support running arbitrary executables; for now
    it only supports :py:class:`~mrjob.job.MRJob` subclasses. Up to v0.5 it is
    effectively part of the :py:class:`~mrjob.job.MRJob` class itself and
    should not be used externally in any way.
    """

    #: :py:class:`optparse.Option` subclass to use with the
    #: :py:class:`optparse.OptionParser` instance.
    OPTION_CLASS = Option

    _DEFAULT_RUNNER = 'local'

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

        self._passthrough_options = []
        self._file_options = []

        self.option_parser = OptionParser(usage=self._usage(),
                                          option_class=self.OPTION_CLASS,
                                          add_help_option=False)
        self.configure_options()

        for opt_group in self.all_option_groups():
            _alphabetize_options(opt_group)

        # don't pass None to parse_args unless we're actually running
        # the MRJob script
        if args is _READ_ARGS_FROM_SYS_ARGV:
            self._cl_args = sys.argv[1:]
        else:
            # don't pass sys.argv to self.option_parser, and have it
            # raise an exception on error rather than printing to stderr
            # and exiting.
            self._cl_args = args or []

            def error(msg):
                raise ValueError(msg)

            if not from_cl:
                self.option_parser.error = error

        self.load_options(self._cl_args)

        # Make it possible to redirect stdin, stdout, and stderr, for testing
        # See sandbox(), below.
        #
        # These should always read/write bytes, not unicode
        if PY2:
            self.stdin = sys.stdin
            self.stdout = sys.stdout
            self.stderr = sys.stderr
        else:
            self.stdin = sys.stdin.buffer
            self.stdout = sys.stdout.buffer
            self.stderr = sys.stderr.buffer

    @classmethod
    def _usage(cls):
        """Command line usage string for this class"""
        return ("usage: mrjob run [script path|executable path|--help]"
                " [options] [input files]")

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
        if self.options.runner == 'emr':
            # avoid requiring dependencies (such as boto) for other runners
            from mrjob.emr import EMRJobRunner
            return EMRJobRunner(**self.emr_job_runner_kwargs())

        elif self.options.runner == 'dataproc':
            from mrjob.dataproc import DataprocJobRunner
            return DataprocJobRunner(**self.dataproc_job_runner_kwargs())

        elif self.options.runner == 'hadoop':
            from mrjob.hadoop import HadoopJobRunner
            return HadoopJobRunner(**self.hadoop_job_runner_kwargs())

        elif self.options.runner == 'inline':
            raise ValueError("inline is not supported in the multi-lingual"
                             " launcher.")

        else:
            # run locally by default
            from mrjob.local import LocalMRJobRunner
            return LocalMRJobRunner(**self.local_job_runner_kwargs())

    @classmethod
    def set_up_logging(cls, quiet=False, verbose=False, stream=None):
        """Set up logging when running from the command line. This is also
        used by the various command-line utilities.

        :param bool quiet: If true, don't log. Overrides *verbose*.
        :param bool verbose: If true, set log level to ``DEBUG`` (default is
                             ``INFO``)
        :param bool stream: Stream to log to (default is ``sys.stderr``)

        This will also set up a null log handler for boto, so we don't get
        warnings if boto tries to log about throttling and whatnot.
        """
        if quiet:
            log_to_null(name='mrjob')
            log_to_null(name='__main__')
        else:
            log_to_stream(name='mrjob', debug=verbose, stream=stream)
            log_to_stream(name='__main__', debug=verbose, stream=stream)

        log_to_null(name='boto')

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
                for line in runner.stream_output():
                    self.stdout.write(line)
                self.stdout.flush()

    ### Command-line arguments ###

    def configure_options(self):
        """Define arguments for this script. Called from :py:meth:`__init__()`.

        Run ``python -m mrjob.job.MRJob --help`` to see all options.

        Re-define to define custom command-line arguments::

            def configure_options(self):
                super(MRYourJob, self).configure_options

                self.add_passthrough_option(...)
                self.add_file_option(...)
                ...
        """
        self.option_parser.add_option(
            '--help', dest='help_main', action='store_true', default=False,
            help='show this message and exit')

        self.option_parser.add_option(
            '--help-dataproc', dest='help_dataproc', action='store_true',
            default=False,
            help='show Dataproc-related options')

        self.option_parser.add_option(
            '--help-emr', dest='help_emr', action='store_true', default=False,
            help='show EMR-related options')

        self.option_parser.add_option(
            '--help-hadoop', dest='help_hadoop', action='store_true',
            default=False,
            help='show Hadoop-related options')

        self.option_parser.add_option(
            '--help-local', dest='help_local', action='store_true',
            default=False,
            help='show local/inline runner-related options')

        self.option_parser.add_option(
            '--help-runner', dest='help_runner', action='store_true',
            default=False, help='show runner-related options')

        # protocol stuff
        self.proto_opt_group = OptionGroup(
            self.option_parser, 'Protocols')
        self.option_parser.add_option_group(self.proto_opt_group)

        _add_protocol_opts(self.proto_opt_group)

        # options for running the entire job
        self.runner_opt_group = OptionGroup(
            self.option_parser, 'Running the entire job')
        self.option_parser.add_option_group(self.runner_opt_group)

        _add_runner_opts(self.runner_opt_group, self._DEFAULT_RUNNER)
        _add_basic_opts(self.runner_opt_group)

        # options for inline/local runners
        self.local_opt_group = OptionGroup(
            self.option_parser,
            'Running locally (these apply when you set -r inline or -r local)')
        self.option_parser.add_option_group(self.local_opt_group)

        _add_local_opts(self.local_opt_group)

        # options common to Hadoop and EMR
        self.hadoop_emr_opt_group = OptionGroup(
            self.option_parser,
            'Running on Hadoop or EMR (these apply when you set -r hadoop or'
            ' -r emr)')
        self.option_parser.add_option_group(self.hadoop_emr_opt_group)

        _add_hadoop_emr_opts(self.hadoop_emr_opt_group)

        # options for running the job on Hadoop
        self.hadoop_opt_group = OptionGroup(
            self.option_parser,
            'Running on Hadoop (these apply when you set -r hadoop)')
        self.option_parser.add_option_group(self.hadoop_opt_group)

        _add_hadoop_opts(self.hadoop_opt_group)

        # options for running the job on Dataproc or EMR
        self.dataproc_emr_opt_group = OptionGroup(
            self.option_parser,
            'Running on Dataproc or EMR (these apply when you set -r dataproc'
            ' or -r emr)')
        self.option_parser.add_option_group(self.dataproc_emr_opt_group)

        _add_dataproc_emr_opts(self.dataproc_emr_opt_group)

        # options for running the job on Dataproc
        self.dataproc_opt_group = OptionGroup(
            self.option_parser,
            'Running on Dataproc (these apply when you set -r dataproc)')
        self.option_parser.add_option_group(self.dataproc_opt_group)

        _add_dataproc_opts(self.dataproc_opt_group)

        # options for running the job on EMR
        self.emr_opt_group = OptionGroup(
            self.option_parser,
            'Running on EMR (these apply when you set -r emr)')
        self.option_parser.add_option_group(self.emr_opt_group)

        _add_emr_opts(self.emr_opt_group)

    def all_option_groups(self):
        return (self.option_parser, self.proto_opt_group,
                self.runner_opt_group, self.hadoop_opt_group,
                self.dataproc_emr_opt_group, self.hadoop_emr_opt_group,
                self.dataproc_opt_group, self.emr_opt_group,
                self.local_opt_group)

    def is_task(self):
        """True if this is a mapper, combiner, or reducer.

        This is mostly useful inside :py:meth:`load_options`, to disable
        loading options when we aren't running inside Hadoop Streaming.
        """
        return False

    def is_mapper_or_reducer(self):
        """The old name for :py:meth:`is_task`. Going away in v0.6.0"""
        log.warning('is_mapper_or_reducer() has been renamed to is_task().'
                    ' This alias will be removed in v0.6.0')
        return self.is_task()

    def add_passthrough_option(self, *args, **kwargs):
        """Function to create options which both the job runner
        and the job itself respect (we use this for protocols, for example).

        Use it like you would use :py:func:`optparse.OptionParser.add_option`::

            def configure_options(self):
                super(MRYourJob, self).configure_options()
                self.add_passthrough_option(
                    '--max-ngram-size', type='int', default=4, help='...')

        Specify an *opt_group* keyword argument to add the option to that
        :py:class:`OptionGroup` rather than the top-level
        :py:class:`OptionParser`.

        If you want to pass files through to the mapper/reducer, use
        :py:meth:`add_file_option` instead.
        """
        if 'opt_group' in kwargs:
            pass_opt = kwargs.pop('opt_group').add_option(*args, **kwargs)
        else:
            pass_opt = self.option_parser.add_option(*args, **kwargs)

        self._passthrough_options.append(pass_opt)

    def add_file_option(self, *args, **kwargs):
        """Add a command-line option that sends an external file
        (e.g. a SQLite DB) to Hadoop::

             def configure_options(self):
                super(MRYourJob, self).configure_options()
                self.add_file_option('--scoring-db', help=...)

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
        pass_opt = self.option_parser.add_option(*args, **kwargs)

        if not pass_opt.type == 'string':
            raise OptionError(
                'passthrough file options must take strings' % pass_opt.type)

        if pass_opt.action not in ('store', 'append'):
            raise OptionError("passthrough file options must use the options"
                              " 'store' or 'append'")

        self._file_options.append(pass_opt)

    def _process_args(self, args):
        """mrjob.launch takes the first arg as the script path, but mrjob.job
        uses all args as input files. This method determines the behavior:
        MRJobLauncher takes off the first arg as the script path.
        """
        if not self._script_path:
            if len(args) < 1:
                self.option_parser.error('Must supply script path')
            else:
                self._script_path = os.path.abspath(args[0])
                self.args = args[1:]

    def _help_main(self):
        self.option_parser.option_groups = []
        self.option_parser.print_help()
        sys.exit(0)

    def load_options(self, args):
        """Load command-line options into ``self.options``,
        ``self._script_path``, and ``self.args``.

        Called from :py:meth:`__init__()` after :py:meth:`configure_options`.

        :type args: list of str
        :param args: a list of command line arguments. ``None`` will be
                     treated the same as ``[]``.

        Re-define if you want to post-process command-line arguments::

            def load_options(self, args):
                super(MRYourJob, self).load_options(args)

                self.stop_words = self.options.stop_words.split(',')
                ...
        """
        self.options, args = self.option_parser.parse_args(args)

        if self.options.help_main:
            self._help_main()

        if self.options.help_dataproc:
            _print_help_for_groups(self.dataproc_emr_opt_group,
                                   self.dataproc_opt_group)
            sys.exit(0)

        if self.options.help_emr:
            _print_help_for_groups(self.dataproc_emr_opt_group,
                                   self.hadoop_emr_opt_group,
                                   self.emr_opt_group)
            sys.exit(0)

        if self.options.help_hadoop:
            _print_help_for_groups(self.hadoop_emr_opt_group,
                                   self.hadoop_opt_group)
            sys.exit(0)

        if self.options.help_local:
            _print_help_for_groups(self.local_opt_group)
            sys.exit(0)

        if self.options.help_runner:
            _print_help_for_groups(self.runner_opt_group)
            sys.exit(0)

        self._process_args(args)

        _fix_custom_options(self.options, self.option_parser)

    def job_runner_kwargs(self):
        """Keyword arguments used to create runners when
        :py:meth:`make_runner` is called.

        :return: map from arg name to value

        Re-define this if you want finer control of runner initialization.

        You might find :py:meth:`mrjob.conf.combine_dicts` useful if you
        want to add or change lots of keyword arguments.
        """
        return {
            'bootstrap_mrjob': self.options.bootstrap_mrjob,
            'check_input_paths': self.options.check_input_paths,
            'cleanup': self.options.cleanup,
            'cleanup_on_failure': self.options.cleanup_on_failure,
            'cmdenv': self.options.cmdenv,
            'conf_paths': self.options.conf_paths,
            'extra_args': self.generate_passthrough_arguments(),
            'file_upload_args': self.generate_file_upload_args(),
            'hadoop_input_format': self.hadoop_input_format(),
            'hadoop_output_format': self.hadoop_output_format(),
            'input_paths': self.args,
            'interpreter': self.options.interpreter,
            'jobconf': self.jobconf(),
            'libjars': self.libjars(),
            'mr_job_script': self._script_path,
            'label': self.options.label,
            'output_dir': self.options.output_dir,
            'owner': self.options.owner,
            'partitioner': self.partitioner() or self.options.partitioner,
            'python_archives': self.options.python_archives,
            'python_bin': self.options.python_bin,
            'setup': self.options.setup,
            'setup_cmds': self.options.setup_cmds,
            'setup_scripts': self.options.setup_scripts,
            'stdin': self.stdin,
            'steps_interpreter': self.options.steps_interpreter,
            'steps_python_bin': self.options.steps_python_bin,
            'strict_protocols': self.options.strict_protocols,
            'upload_archives': self.options.upload_archives,
            'upload_files': self.options.upload_files,
        }

    def inline_job_runner_kwargs(self):
        """Keyword arguments to create create runners when
        :py:meth:`make_runner` is called, when we run a job locally
        (``-r inline``).

        :return: map from arg name to value

        Re-define this if you want finer control when running jobs locally.
        """
        return combine_dicts(
            self.job_runner_kwargs(),
            self._get_kwargs_from_opt_group(self.local_opt_group))

    def local_job_runner_kwargs(self):
        """Keyword arguments to create create runners when
        :py:meth:`make_runner` is called, when we run a job locally
        (``-r local``).

        :return: map from arg name to value

        Re-define this if you want finer control when running jobs locally.
        """
        return combine_dicts(
            self.job_runner_kwargs(),
            self._get_kwargs_from_opt_group(self.local_opt_group))

    def emr_job_runner_kwargs(self):
        """Keyword arguments to create create runners when
        :py:meth:`make_runner` is called, when we run a job on EMR
        (``-r emr``).

        :return: map from arg name to value

        Re-define this if you want finer control when running jobs on EMR.
        """
        return combine_dicts(
            self.job_runner_kwargs(),
            self._get_kwargs_from_opt_group(self.dataproc_emr_opt_group),
            self._get_kwargs_from_opt_group(self.emr_opt_group),
            self._get_kwargs_from_opt_group(self.hadoop_emr_opt_group))

    def dataproc_job_runner_kwargs(self):
        """Keyword arguments to create create runners when
        :py:meth:`make_runner` is called, when we run a job on EMR
        (``-r emr``).

        :return: map from arg name to value

        Re-define this if you want finer control when running jobs on EMR.
        """
        return combine_dicts(
            self.job_runner_kwargs(),
            self._get_kwargs_from_opt_group(self.dataproc_emr_opt_group),
            self._get_kwargs_from_opt_group(self.dataproc_opt_group))

    def hadoop_job_runner_kwargs(self):
        """Keyword arguments to create create runners when
        :py:meth:`make_runner` is called, when we run a job on EMR
        (``-r hadoop``).

        :return: map from arg name to value

        Re-define this if you want finer control when running jobs on hadoop.
        """
        return combine_dicts(
            self.job_runner_kwargs(),
            self._get_kwargs_from_opt_group(self.hadoop_emr_opt_group),
            self._get_kwargs_from_opt_group(self.hadoop_opt_group))

    ### Default values for Hadoop stuff ###

    def hadoop_input_format(self):
        """Hadoop input format must be known by the launcher but is defined by
        the job. We have no good way to do this, but
        :py:class:`~mrjob.job.MRJob` handles it well for now.
        """
        return None

    def hadoop_output_format(self):
        """See hadoop_input_format()."""
        return None

    def jobconf(self):
        """See hadoop_input_format()."""
        return {}

    def libjars(self):
        """See hadoop_input_format()."""
        return []

    def partitioner(self):
        """See hadoop_input_format()."""
        return None

    ### More option stuff ###

    def _get_kwargs_from_opt_group(self, opt_group):
        """Helper function that returns a dictionary of the values of options
        in the given options group (this works because the options and the
        keyword args we want to set have identical names).
        """
        keys = set(opt.dest for opt in opt_group.option_list)
        return dict((key, getattr(self.options, key)) for key in keys
                    if key not in _FAKE_OPTIONS)

    def generate_passthrough_arguments(self):
        """Returns a list of arguments to pass to subprocesses, either on
        hadoop or executed via subprocess.

        These are passed to :py:meth:`mrjob.runner.MRJobRunner.__init__`
        as *extra_args*.
        """
        arg_map = parse_and_save_options(self.option_parser, self._cl_args)
        output_args = []

        passthrough_dests = sorted(
            set(option.dest for option in self._passthrough_options))
        for option_dest in passthrough_dests:
            output_args.extend(arg_map.get(option_dest, []))

        return output_args

    def generate_file_upload_args(self):
        """Figure out file upload args to pass through to the job runner.

        Instead of generating a list of args, we're generating a list
        of tuples of ``('--argname', path)``

        These are passed to :py:meth:`mrjob.runner.MRJobRunner.__init__`
        as ``file_upload_args``.
        """
        file_upload_args = []

        master_option_dict = self.options.__dict__

        for opt in self._file_options:
            opt_prefix = opt.get_opt_string()
            opt_value = master_option_dict[opt.dest]

            if opt_value:
                paths = opt_value if opt.action == 'append' else [opt_value]
                for path in paths:
                    file_upload_args.append((opt_prefix, path))

        return file_upload_args

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
