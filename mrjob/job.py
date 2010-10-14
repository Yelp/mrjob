# Copyright 2009-2010 Yelp
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

"""Python implementation of a Hadoop streaming job that encapsulates
one or more mappers and reducers, to run in sequence.

See MRJob's docstring for typical usage.
"""
# don't add imports here that aren't part of the standard Python library,
# since MRJobs need to run in Amazon's generic EMR environment
from __future__ import with_statement

import inspect
import itertools
import logging
from optparse import OptionParser, OptionGroup, OptionError
import sys
import time

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

# don't use relative imports, to allow this script to be invoked as __main__
from mrjob.conf import combine_dicts
from mrjob.parse import parse_mr_job_stderr
from mrjob.protocol import DEFAULT_PROTOCOL, PROTOCOL_DICT
from mrjob.runner import CLEANUP_CHOICES, CLEANUP_DEFAULT
from mrjob.util import log_to_stream, read_input

# used by mr() below, to fake no mapper
def _IDENTITY_MAPPER(key, value):
    yield key, value

# sentinel value; used when running MRJob as a script
_READ_ARGS_FROM_SYS_ARGV = '_READ_ARGS_FROM_SYS_ARGV'

class MRJob(object):
    """The base class for any map reduce job. Handles argument parsing,
    spawning of sub processes and/or hadoop jobs, and creating of temporary files.

    To create your own map reduce job, create a series of mappers and reducers,
    override the steps() method. For example, a word counter:

    class WordCounter(MRJob):
        def get_words(self, key, line):
            for word in line.split():
                yield word, 1

        def sum_words(self, word, occurrences):
            yield word, sum(occurrences)

        def steps(self):
            return [self.mr(self.get_words, self.sum_words),]

    if __name__ == '__main__':
        WordCounter.run()

    The two lines at the bottom are mandatory; this is what allows your
    class to be run by hadoop streaming.

    This will take in a file with lines of whitespace separated words, and
    output a file where each line is "(word)\t(count)"

    For single-step jobs, you can also just redefine mapper() and reducer():

    class WordCounter(MRJob):
        def mapper(self, key, line):
            for word in line.split():
                yield word, 1

        def reducer(self, word, occurrences):
            yield word, sum(occurrences)

    if __name__ == '__main__':
        WordCounter.run()

    You can override configure_options() to add command-line arguments to your
    script. Any arguments that needs to get passed to your mappers / reducers
    should be set in arguments.

    To test the job locally, just run:

    python your_mr_job_sub_class.py < log_file_or_whatever > output

    The script will automatically invoke itself to run the various steps,
    using LocalMRJobRunner.

    You can also test individual steps:

    # test 1st step mapper:
    python your_mr_job_sub_class.py --mapper 
    # test 2nd step reducer (--step-num=1 because step numbers are 0-indexed):
    python your_mr_job_sub_class.py --reducer --step-num=1

    By default, we read from stdin, but you can also specify one or more input
files. It automatically decompresses .gz and .bz2 files:

    python your_mr_job_sub_class.py log_01.gz log_02.bz2 log_03

    You can run on Amazon Elastic MapReduce by specifying --runner emr or on your
    own Hadoop cluster by specifying --runner hadoop.

    To run an MRJob from within another python process:

    mr_job = YourMrJob(args) # specify cmd line args, as a list
    with mr_job.make_runner() as runner:
        runner.run()
        for line in runner.stream_output():
            key, value = mr_job.parse_output_line(line)
            ... # do something with the parsed output

    (in Python 2.5, use from __future__ import with_statement)
    """
    def __init__(self, args=None):
        """Set up option parsing."""
        # make sure we respect the $TZ (time zone) environment variable
        time.tzset()

        self._passthrough_options = []
        self._file_options = []

        usage = "usage: %prog [options] [input files]"
        self.option_parser = OptionParser(usage=usage)
        self.configure_options()

        # Load and validate options
        self.load_options(args=args)

        # Make it possible to redirect stdin, stdout, and stderr, for testing
        # See sandbox(), below.
        self.stdin = sys.stdin
        self.stdout = sys.stdout
        self.stderr = sys.stderr

    ### stuff to redefine in your process ###

    # either define mapper(), mapper_final(), and/or reducer(), or
    # redefine steps() (and point it at whatever methods you like)

    #def mapper(self, key, value):
    
    #def mapper_final(self):

    #def reducer(self, key, values):

    def steps(self):
        """Return a list of mappers/reducers. Use MRJob.mr() to construct
        these.
        """
        # Use mapper(), mapper_final(), and reducer() if they're implemented
        kwargs = dict((func_name, getattr(self, func_name, None))
                      for func_name in ('mapper', 'mapper_final', 'reducer'))

        return [self.mr(**kwargs)]

    # you may also wish to add on to self.configure_options()

    # Don't redefine this; use it inside steps()
    @classmethod
    def mr(cls, mapper=None, reducer=None, mapper_final=None):
        """Return a description of a single map-reduce step to be returned
        by steps() (Currently this produces a tuple, but you should consider the
        format opaque, and subject to change).

        Args:
        mapper -- function that takes (key, value) and yields zero or
            more (key, value) pairs. If mapper is None, we will still run a
            mapper (Hadoop Streaming requires us to), but it will just pass
            through key and value. This is not usually useful in the first
            step of a job.
        reducer -- function that takes (key, values) and yields zero or
            more (key, value) pairs. values is a stream of values. If this
            is None, we won't run a reducer at all.
        mapper_final -- a function that runs after the final key, value pair is
            sent to mapper; takes no args, and yields zero or more
            (key, value) pairs. Please invoke this as a keyword arg.
        """
        # Hadoop streaming requires a mapper, so patch in _IDENTITY_MAPPER
        if not mapper:
            mapper = _IDENTITY_MAPPER

        if mapper_final:
            return ((mapper, mapper_final), reducer)
        else:
            return (mapper, reducer)

    ### running the job ###

    @classmethod
    def run(cls):
        """Construct the steps, parse the options, and launch the job.

        This is the entry point when the job is run in hadoop streaming
        """
        # load options from the command line
        mr_job = cls(args=_READ_ARGS_FROM_SYS_ARGV)
        
        if mr_job.options.show_steps:
            mr_job.show_steps()

        elif mr_job.options.run_mapper:
            mr_job.run_mapper()

        elif mr_job.options.run_reducer:
            mr_job.run_reducer()

        else:
            mr_job.run_job()

    def is_mapper_or_reducer(self):
        """True if this is a mapper/reducer."""
        return self.options.run_mapper or self.options.run_reducer

    def show_steps(self):
        """Print information about how many mappers and reducers there are."""
        print >> self.stdout, ' '.join(self.steps_desc())

    def run_mapper(self):
        step_num = self.options.step_num or 0
        steps = self.steps()
        if not 0 <= step_num < len(steps):
            raise ValueError('Out-of-range step: %d' % step_num)
        mapper = steps[step_num][0]

        # special case: mapper is actually a tuple of mapper and final mapper
        if isinstance(mapper, tuple):
            mapper, mapper_final = mapper
        else:
            mapper_final = None

        # pick input and output protocol
        read_lines, write_line = self.wrap_protocols(step_num, 'M')

        # run the mapper on each line
        for key, value in read_lines():
            for out_key, out_value in mapper(key, value):
                write_line(out_key, out_value)

        if mapper_final:
            for out_key, out_value in mapper_final():
                write_line(out_key, out_value)

    def run_reducer(self):
        step_num = self.options.step_num or 0
        steps = self.steps()
        if not 0 <= step_num < len(steps):
            raise ValueError('Out-of-range step: %d' % step_num)
        reducer = steps[step_num][1]
        if reducer is None:
            raise ValueError('No reducer in step %d' % step_num)

        # pick input and output protocol
        read_lines, write_line = self.wrap_protocols(step_num, 'R')

        # group all values of the same key together, and pass to the reducer
        #
        # be careful to use generators for everything, to allow for
        # very large groupings of values
        for key, kv_pairs in itertools.groupby(read_lines(),
                                               key=lambda(k, v): k):
            values = (v for k, v in kv_pairs)
            for out_key, out_value in reducer(key, values):
                write_line(out_key, out_value)

    def run_job(self):
        """Run the job, logging errors (and debugging output if --verbose is
        specified) to STDERR and streaming the output to STDOUT. Cleanup as
        directed by the --cleanup option.

        If you want to run the job from within another Python process, you
        should just call self.make_runner(), and then call
        run(), stream_output() and cleanup() on the job runner.
        """
        log_to_stream(
            name='mrjob', stream=self.stderr, debug=self.options.verbose)

        with self.make_runner() as runner:
            runner.run()
            for line in runner.stream_output():
                self.stdout.write(line)
            self.stdout.flush()

    def make_runner(self):
        """Look at options, and return an appropriate job runner.
        """
        # have to import here so that we can still run the MRJob
        # without importing boto
        from mrjob.emr import EMRJobRunner
        from mrjob.hadoop import HadoopJobRunner
        from mrjob.local import LocalMRJobRunner
        
        if self.options.runner == 'emr':
            return EMRJobRunner(**self.emr_job_runner_kwargs())

        elif self.options.runner == 'hadoop':
            return HadoopJobRunner(**self.hadoop_job_runner_kwargs())

        else:
            # run locally by default
            return LocalMRJobRunner(**self.local_job_runner_kwargs())

    @classmethod
    def mr_job_script(cls):
        """Path of this script. This returns the file containing
        this class."""
        return inspect.getsourcefile(cls)
    
    def job_runner_kwargs(self):
        """General keyword arguments to construct an MRJobRunner"""
        return {
            'cleanup': self.options.cleanup,
            'conf_path': self.options.conf_path,
            'extra_args': self.generate_passthrough_arguments(),
            'file_upload_args': self.generate_file_upload_args(),
            'input_paths': self.args,
            'jobconf_args': self.options.jobconf_args,
            'mr_job_script': self.mr_job_script(),
            'output_dir': self.options.output_dir,
            'stdin': self.stdin,
            'upload_archives': self.options.upload_archives,
            'upload_files': self.options.upload_files,
        }

    def local_job_runner_kwargs(self):
        """Keyword arguments to construct a LocalMRJobRunner."""
        return self.job_runner_kwargs()

    def emr_job_runner_kwargs(self):
        """Keyword arguments to construct an EMRJobRunner."""
        return combine_dicts(
            self.job_runner_kwargs(),
            self._get_kwargs_from_opt_group(self.emr_opt_group))

    def hadoop_job_runner_kwargs(self):
        """Keyword arguments to construct a HadoopJobRunner."""
        return combine_dicts(
            self.job_runner_kwargs(),
            self._get_kwargs_from_opt_group(self.hadoop_opt_group))

    def _get_kwargs_from_opt_group(self, opt_group):
        """Helper function that returns a dictionary of the values of options
        in the given options group (this works because the options and the
        keyword args we want to set have identical names).
        """
        keys = set(opt.dest for opt in opt_group.option_list)
        return dict((key, getattr(self.options, key)) for key in keys)

    ### Other useful utilities ###

    def wrap_protocols(self, step_num, step_type):
        """Pick the protocol classes to use for reading and writing
        for the given step, and wrap them so that bad input and output
        trigger a counter rather than an exception.

        Returns a tuple of read_lines, write_line
        read_lines() is a function that reads lines from input, decodes them,
            and yields key, value pairs
        write_line() is a function that takes key and value as args, encodes
            them, and writes a line to output.

        Args:
        step_num -- which step to run (e.g. 0)
        step_type -- 'M' for mapper, 'R' for reducer
        """
        read, write = self.pick_protocols(step_num, step_type)

        def read_lines():
            for line in self.read_input():
                try:
                    key, value = read(line.rstrip('\n'))
                    yield key, value
                except Exception, e:
                    self.increment_counter('Undecodable input',
                                           e.__class__.__name__)

        def write_line(key, value):
            try:
                print >> self.stdout, write(key, value)
            except Exception, e:
                self.increment_counter('Unencodable output',
                                       e.__class__.__name__)

        return read_lines, write_line

    def read_input(self):
        """Read from stdin, or one more files, or directories.
        Yield one line at time.

        - Resolve globs ('foo_*.gz').
        - Decompress .gz and .bz2 files.
        - If path is '-', read from STDIN.
        - Recursively read all files in a directory
        """
        paths = self.args or ['-']
        for path in paths:
            for line in read_input(path, stdin=self.stdin):
                yield line

    def pick_protocols(self, step_num, step_type):
        """Pick the protocol classes to use for reading and writing
        for the given step.

        step_num -- which step to run (e.g. 0)
        step_type -- 'M' for mapper, 'R' for reducer
        """
        steps_desc = self.steps_desc()

        protocol_dict = self.protocols()
        
        # pick input protocol
        if step_num == 0 and step_type == steps_desc[0][0]:
            read_protocol = self.options.input_protocol
        else:
            read_protocol = self.options.protocol
        read = protocol_dict[read_protocol].read

        if step_num == len(steps_desc) - 1 and step_type == steps_desc[-1][-1]:
            write_protocol = self.options.output_protocol
        else:
            write_protocol = self.options.protocol
        write = protocol_dict[write_protocol].write

        return read, write

    def steps_desc(self):
        return ['MR' if reducer else 'M' for (mapper, reducer) in self.steps()]

    ### Command-line arguments ###

    def load_options(self, args):
        """Load options. args should be a list of command line arguments.

        If you want to load args from sys.argv, explicilty set args
        to sys.argv[1:]
        """
        # don't pass None to parse_args unless we're actually running
        # the MRJob script
        if args is _READ_ARGS_FROM_SYS_ARGV:
            self.options, self.args = self.option_parser.parse_args()
        else:
            # don't pass sys.argv to self.option_parser, and have it
            # raise an exception on error rather than printing to stderr
            # and exiting.
            args = args or []
            def error(msg):
                raise ValueError(msg)
            self.option_parser.error = error
            self.options, self.args = self.option_parser.parse_args(args)

        # output_protocol defaults to protocol
        if not self.options.output_protocol:
            self.options.output_protocol = self.options.protocol

    def configure_options(self):
        """Define arguments for this function. Inherit from this and add additional
        options if you need to accept command line arguments
        """
        # To describe the steps
        self.option_parser.add_option(
            '--steps', dest='show_steps', action='store_true', default=False,
            help='show the steps of mappers and reducers')

        # To run mappers or reducers
        self.option_parser.add_option(
            '--mapper', dest='run_mapper', action='store_true', default=False,
            help='run a mapper')

        self.option_parser.add_option(
            '--reducer', dest='run_reducer', action='store_true', default=False,
            help='run a reducer')

        self.option_parser.add_option(
            '--step-num', dest='step_num', type='int', default=0, default=False,
            help='which step to execute (default is 0)')

        # protocol stuff
        protocol_choices = sorted(self.protocols())

        self.add_passthrough_option(
            '-p', '--protocol', dest='protocol',
            default=self.DEFAULT_PROTOCOL, choices=protocol_choices,
            help='output protocol for mappers/reducers. Choices: %s (default: %%default)' % ', '.join(protocol_choices))
        self.add_passthrough_option(
            '--output-protocol', dest='output_protocol',
            default=self.DEFAULT_OUTPUT_PROTOCOL,
            choices=protocol_choices,
             help='protocol for final output (default: %s)' % (
            'same as --protocol' if self.DEFAULT_OUTPUT_PROTOCOL is None
            else '%default'))
        self.add_passthrough_option(
            '--input-protocol', dest='input_protocol',
            default=self.DEFAULT_INPUT_PROTOCOL, choices=protocol_choices,
            help='protocol to read input with (default: %default)')

        # options for running the entire job
        self.runner_opt_group = OptionGroup(
            self.option_parser, 'Running the entire job')
        self.option_parser.add_option_group(self.runner_opt_group)
        
        self.runner_opt_group.add_option(
            '--runner', dest='runner', default='local',
            choices=('local', 'hadoop', 'emr'),
            help='Where to run the job: local to run locally, hadoop to run on your Hadoop cluster, emr to run on Amazon ElasticMapReduce. Default is local.')
        self.runner_opt_group.add_option(
            '-c', '--conf-path', dest='conf_path', default=None,
            help='Path to alternate mrjob.conf file to read from')
        self.runner_opt_group.add_option(
            '--no-conf', dest='conf_path', action='store_false',
            help="Don't load mrjob.conf even if it's available")
        self.runner_opt_group.add_option(
            '--cleanup', dest='cleanup',
            choices=CLEANUP_CHOICES, default=CLEANUP_DEFAULT,
            help="when to clean up tmp directories, etc. Choices: %s (default: %%default)" % ', '.join(CLEANUP_CHOICES))
        self.runner_opt_group.add_option(
            '-v', '--verbose', dest='verbose', default=False,
            action='store_true',
            help='print more messages to stderr')
        self.runner_opt_group.add_option(
            '-q', '--quiet', dest='quiet', default=False,
            action='store_true',
            help="Don't print anything to stderr")
        self.runner_opt_group.add_option(
            '--file', dest='upload_files', action='append',
            default=[],
            help='copy file to the working directory of this script')
        self.runner_opt_group.add_option(
            '--archive', dest='upload_archives', action='append',
            default=[],
            help='unpack archive in the working directory of this script')

        self.runner_opt_group.add_option(
            '-o', '--output-dir', dest='output_dir', default=None,
            help='Where to put final job output. This must be an s3:// URL ' +
            'for EMR, and an HDFS path for Hadoop, and must be empty')

        self.runner_opt_group.add_option(
            '--job-name-prefix', dest='job_name_prefix', default=None,
            help='custom prefix for job name, to help us identify the job')

        self.runner_opt_group.add_option(
            '--jobconf', dest='jobconf_args', default=[], action='append',
            help='-jobconf arg to pass through to hadoop streaming; should take the form KEY=VALUE')

        self.runner_opt_group.add_option(
            '--hadoop-arg', dest='hadoop_arg', default=[], action='append',
            help='Argument of any type to pass to hadoop streaming (you can use --hadoop arg multiple times)')

        # options for running the job on Hadoop
        self.hadoop_opt_group = OptionGroup(
            self.option_parser, 'Running on Hadoop (these apply when you set --runner hadoop)')
        self.option_parser.add_option_group(self.hadoop_opt_group)

        self.hadoop_opt_group.add_option(
            '--hadoop-bin', dest='hadoop_bin', default=None,
            help='hadoop binary. Defaults to $HADOOP_HOME/bin/hadoop')
        self.hadoop_opt_group.add_option(
            '--hadoop-streaming-jar', dest='hadoop_streaming_jar',
            default=None,
            help='Path of your hadoop streaming jar (REQUIRED)')
        self.hadoop_opt_group.add_option(
            '--hdfs-scratch-dir', dest='hdfs_scratch_dir',
            default=None,
            help='Scratch space on HDFS (default is tmp/)')

        # options for running the job on EMR
        self.emr_opt_group = OptionGroup(
            self.option_parser, 'Running on Amazon Elastic MapReduce (these apply when you set --runner emr)')
        self.option_parser.add_option_group(self.emr_opt_group)

        self.emr_opt_group.add_option(
            '--ec2-instance-type', dest='ec2_instance_type', default=None,
            help='Type of EC2 instance(s) to launch (e.g. m1.small, c1.xlarge, m2.xlarge). See http://aws.amazon.com/ec2/instance-types/ for the full list.')
        self.emr_opt_group.add_option(
            '--ec2-master-instance-type', dest='ec2_master_instance_type', default=None,
            help='Type of EC2 instance for master node only')
        self.emr_opt_group.add_option(
            '--ec2-slave-instance-type', dest='ec2_slave_instance_type', default=None,
            help='Type of EC2 instance for slave nodes only')
        self.emr_opt_group.add_option(
            '--num-ec2-instances', dest='num_ec2_instances', default=None,
            type='int',
            help='Number of EC2 instances to launch')
        self.emr_opt_group.add_option(
            '--s3-scratch-uri', dest='s3_scratch_uri', default=None,
            help='URI on S3 to use as our temp directory.')
        self.emr_opt_group.add_option(
            '--s3-log-uri', dest='s3_log_uri', default=None,
            help='URI on S3 to write logs into')
        self.emr_opt_group.add_option(
            '--check-emr-status-every', dest='check_emr_status_every',
            default=None, type='int',
            help='How often (in seconds) to check status of your EMR job')
        self.emr_opt_group.add_option(
            '--ssh-tunnel-to-job-tracker', dest='ssh_tunnel_to_job_tracker',
            default=None, action='store_true',
            help='Open up an SSH tunnel to the Hadoop job tracker')
        self.emr_opt_group.add_option(
            '--ssh-tunnel-is-open', dest='ssh_tunnel_is_open',
            default=None, action='store_true',
            help='Make ssh tunnel accessible from remote hosts (not just localhost)')
        self.emr_opt_group.add_option(
            '--emr-job-flow-id', dest='emr_job_flow_id', default=None,
            help='ID of an existing EMR job flow to use')

    def add_passthrough_option(self, *args, **kwargs):
        """Function to create options which both the job runner
        and the job itself respect (we use this for protocols, for example).

        Use it like you would use self.option_parser.add_option():

        def configure_options(self):
            super(YourClass, self).configure_options()
            self.add_passthrough_option('--foo', dest='foo', ...)
            
        If you want to pass files through to the mapper/reducer, use
        add_file_option() instead.
        """
        pass_opt = self.option_parser.add_option(*args, **kwargs)
        
        # We only support a subset of option parser actions
        SUPPORTED_ACTIONS = (
            'store', 'append', 'store_const', 'store_true', 'store_false',)
        if not pass_opt.action in SUPPORTED_ACTIONS:
            raise OptionError('Expecting only actions %s, got %r' % (SUPPORTED_ACTIONS, pass_opt.action))

        # We only support a subset of option parser choices
        SUPPORTED_TYPES = ('int', 'long', 'float', 'string', 'choice', None)
        if not pass_opt.type in SUPPORTED_TYPES:
            raise OptionError('Expecting only types %s, got %r' % (SUPPORTED_TYPES, pass_opt.type))

        self._passthrough_options.append(pass_opt)

    def add_file_option(self, *args, **kwargs):
        """Add an option that takes a file.

        This does the right thing: the file will be uploaded to the working
        dir of the script, and the script will be passed the same option, but
        with the local name of the file in the script's working directory.
        """
        pass_opt = self.option_parser.add_option(*args, **kwargs)

        if not pass_opt.type == 'string':
            raise OptionError('passthru file options must take strings' % pass_opt.type)

        if not pass_opt.action in ('store', 'append'):
            raise OptionError('passthru file options must have store or append as their actions')

        self._file_options.append(pass_opt)

    def generate_passthrough_arguments(self):
        """Returns a list of arguments to pass to subprocesses, either on hadoop
        or executed via subprocess
        """
        master_option_dict = self.options.__dict__
        
        output_args = []
        for pass_opt in self._passthrough_options:
            opt_prefix = pass_opt.get_opt_string()
            opt_value = master_option_dict[pass_opt.dest]

            # Pass through the arguments for these actions
            if pass_opt.action == 'store' and opt_value is not None:
                output_args.append(opt_prefix)
                output_args.append(str(opt_value))
            elif pass_opt.action == 'append':
                for value in opt_value:
                    output_args.append(opt_prefix)
                    output_args.append(str(value))
            if pass_opt.action == 'store_true' and opt_value == True:
                output_args.append(opt_prefix)
            elif pass_opt.action == 'store_false' and opt_value == False:
                output_args.append(opt_prefix)
            elif pass_opt.action == 'store_const' and opt_value is not None:
                output_args.append(opt_prefix)

        return output_args

    def generate_file_upload_args(self):
        """Figure out file upload args to pass through to the job runner.

        Instead of generating a list of args, we're generating a list
        of tuples of ('--argname', path)
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

    def increment_counter(self, group, counter, amount=1):
        """Increment a hadoop counter in hadoop streaming by printing to stderr.
        """
        # don't allow people to pass in floats
        if not isinstance(amount, (int, long)):
            raise TypeError('amount must be an integer, not %r' % (amount,))

        # Extra commas screw up hadoop and there's no way to escape them. So
        # replace them with the next best thing: semicolons!
        #
        # cast to str() because sometimes people pass in exceptions or whatever
        #
        # The relevant Hadoop code is incrCounter(), here:
        # http://svn.apache.org/viewvc/hadoop/mapreduce/trunk/src/contrib/streaming/src/java/org/apache/hadoop/streaming/PipeMapRed.java?view=markup
        group = str(group).replace(',', ';')
        counter = str(counter).replace(',', ';')

        self.stderr.write('reporter:counter:%s,%s,%d\n' %
                          (group, counter, amount))
        self.stderr.flush()

    def set_status(self, msg):
        """Set the job status in hadoop streaming by printing to stderr.

        This is also a good way of doing a keepalive for a job that goes
        a long time between outputs.
        """
        self.stderr.write('reporter:status:%s\n' % (msg,))
        self.stderr.flush()

    ### protocols ###

    # to add a protocol, define a subclass of HadoopStreamingProtocol,
    # and add it to cls.protocols(), like this:

    # @classmethod
    # def protocols(cls):
    #     protocol_dict = super(YourMRJobClass, cls).protocols()
    #     protocol_dict['custom'] = YourCustomProtocolClass
    #     return protocol_dict

    # then set DEFAULT*_PROTOCOL class varibles accordingly
    
    # Protocols must implement read(cls, line) and write(cls, key, value)
    # see HadoopStreamingProtocol in protocol.py

    DEFAULT_PROTOCOL = DEFAULT_PROTOCOL # 'json'
    DEFAULT_INPUT_PROTOCOL = 'raw_value'
    DEFAULT_OUTPUT_PROTOCOL = None # same as DEFAULT_PROTOCOL

    @classmethod
    def protocols(cls):
        """Mapping from protocol name to the protocol class to use."""
        return PROTOCOL_DICT.copy() # copy to stop monkey-patching

    def parse_output_line(self, line):
        """Parse a line that's the final output of this MRJob, usually into
        (key, value)."""
        reader = self.protocols()[self.options.output_protocol]
        return reader.read(line)

    ### Testing ###
    
    def sandbox(self, stdin=None, stdout=None, stderr=None):
        """Redirect stdin, stdout, and stderr, for ease of testing.

        You can set stdin, stdout, and stderr to file objects. By
        default, they'll be set to StringIOs.

        You can then access the job's file handles through self.stdin,
        self.stdout, and self.stderr. You can use parse_counters(), below,
        to read counters from stderr, or mrjob.util.parse_mr_job_stderr()
        for more complex testing.

        You may call sandbox multiple times (this will essentially clear
        the file handles).

        stdin is empty by default. You can set it to anything that yields
        lines:

        mr_job.sandbox(stdin=StringIO('some_data\n'))

        or, equivalently

        mr_job.sandbox(stdin=['some_data\n'])

        For convenience, this sandbox() returns self, so you can do:

        mr_job = MRJobClassToTest().sandbox()

        See mrjob.tests.job_test for sample test code.
        """
        self.stdin = stdin or StringIO()
        self.stdout = stdout or StringIO()
        self.stderr = stderr or StringIO()

        return self

    def parse_counters(self):
        """Convenience method for reading counters. This only works
        in sandbox mode. This does not clear stderr (safe to call multiple
        times)"""
        if self.stderr == sys.stderr:
            raise AssertionError('You must call sandbox() first; parse_counters() is for testing only.')

        return parse_mr_job_stderr(self.stderr.getvalue())['counters']

    def parse_output(self, protocol=DEFAULT_PROTOCOL):
        """Convenience method for reading output. Return output, as a list
        of tuples.

        Args:
        protocol -- the protocol to use (e.g. 'json'). We don't set this
            automatically to output_protocol so that you can use it to
            test intermediate output as well.

        This only works in sandbox mode. This does not clear stdout (safe to
        call multiple times)"""
        if self.stdout == sys.stdout:
            raise AssertionError('You must call sandbox() first; parse_output() is for testing only.')

        reader = self.protocols()[protocol]
        lines = StringIO(self.stdout.getvalue())
        return [reader.read(line) for line in lines]

if __name__ == '__main__':
    MRJob.run()

