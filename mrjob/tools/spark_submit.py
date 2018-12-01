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
"""Submit a spark job using mrjob runners."""
from argparse import ArgumentParser
from logging import getLogger

from mrjob.conf import combine_lists
from mrjob.job import MRJob
from mrjob.options import _RUNNER_OPTS
from mrjob.options import _add_basic_args
from mrjob.options import _add_runner_args
from mrjob.options import _parse_raw_args
from mrjob.runner import _runner_class
from mrjob.step import SparkJarStep
from mrjob.step import SparkScriptStep

log = getLogger(__name__)


_USAGE = ('%(prog)s spark-submit [-r <runner>] [options]'
          ' <python file | app jar> [app arguments]')

_DESCRIPTION = 'Submit a spark job using mrjob runners'

# for spark-submit args, just need switches and help message
# (which can be patched into runner opts with same dest name)

# then add runner opts (other than check_input_paths) but don't
# display in default help message

# the only runners that support spark scripts/jars
_SPARK_RUNNERS = ('dataproc', 'emr', 'hadoop')

# the default spark runner to use
_DEFAULT_RUNNER = 'hadoop'


_SPARK_SUBMIT_ARG_GROUPS = [
    (None, [
        'spark_master',
        'spark_deploy_mode',
        'main_class',
        'name',
        'libjars',
        'packages',
        'exclude_packages',
        'repositories',
        'py_files',
        'upload_files',
        'jobconf',
        'properties_file',
        'driver_memory',
        'driver_java_options',
        'driver_library_path',
        'driver_class_path',
        'executor_memory',
        'proxy_user',
    ]),
    ('Cluster deploy mode only', [
        'driver_cores',
    ]),
    ('Spark standalone or Mesos with cluster deploy mode only', [
        'supervise',
        # --kill and --status aren't for launching jobs
    ]),
    ('Spark standalone and Mesos only', [
        'total_executor_cores',
    ]),
    ('Spark standalone and YARN only', [
        'executor_cores',
    ]),
    ('YARN-only', [
        'queue_name',
        'num_executors',
        'upload_archives',
        'principal',
        'keytab',
    ]),
]

_SPARK_SUBMIT_OPT_NAMES = {
    opt_name for _, opt_names in _SPARK_SUBMIT_ARG_GROUPS
    for opt_name in opt_names
}


_SPARK_SUBMIT_SWITCHES = dict(
    driver_class_path='--driver-class-path',
    driver_cores='--driver-cores',
    driver_java_options='--driver-java-options',
    driver_library_path='--driver-library-path',
    driver_memory='--driver-memory',
    exclude_packages='--exclude-packages',
    executor_cores='--executor-cores',
    executor_memory='--executor-memory',
    jobconf='--conf',
    keytab='--keytab',
    libjars='--jars',
    main_class='--class',
    name='--name',
    num_executors='--num-executors',
    packages='--packages',
    principal='--principal',
    properties_file='--properties-file',
    proxy_user='--proxy-user',
    py_files='--py-files',
    queue_name='--queue',
    repositories='--repositories',
    spark_deploy_mode='--deploy-mode',
    spark_master='--master',
    supervise='--supervise',
    total_executor_cores='--total-executor-cores',
    upload_archives='--archives',
    upload_files='--files',
)

# not a runner opt or one that's passed straight through to spark
_STEP_OPT_NAMES = {'main_class'}

# arguments that are passed straight through to spark-submit
_SPARK_ARG_OPT_NAMES = (
    set(_SPARK_SUBMIT_SWITCHES) - set(_RUNNER_OPTS) - _STEP_OPT_NAMES)



_SWITCH_ALIASES = {
    '--master': '--spark-master',
    '--deploy-mode': '--spark-deploy-mode',
    '--jars': '--libjars',
    '--conf': '--jobconf',
}


# these options don't make any sense with Spark scripts
_HARD_CODED_OPTS = dict(
    check_input_paths=False
)

# these only work on inline/local runners, which we don't support
_IRRELEVANT_OPT_NAMES = {'hadoop_version', 'num_cores', 'sort_bin'}


def main(cl_args=None):
    parser = _make_arg_parser()
    options = parser.parse_args(cl_args)

    MRJob.set_up_logging(
        quiet=options.quiet,
        verbose=options.verbose,
    )

    runner_class = _runner_class(options.runner)

    kwargs = _get_runner_opt_kwargs(options, runner_class)
    kwargs['step'] = _get_step(options)
    kwargs['spark_args'] = combine_lists(
        kwargs.get('spark_args'), _get_spark_args(parser, cl_args))
    kwargs.update(_HARD_CODED_OPTS)

    runner = runner_class(**kwargs)

    runner.run()


def _get_runner_kwargs(options, parser, runner_class):
    kwargs = _get_runner_opt_kwargs(options, runner_class)

    kwargs['step'] = _get_step(options)

    kwargs['spark_args'] = combine_lists(
        kwargs.get('spark_args'), _get_spark_args(parser, cl_args))

    kwargs.update(_HARD_CODED_OPTS)

    return kwargs


def _get_runner_opt_kwargs(options, runner_class):
    """Extract the options for the given runner class from *options*."""
    return {opt_name: getattr(options, opt_name)
            for opt_name in runner_class.OPT_NAMES
            if hasattr(options, opt_name)}


def _get_step(options):
    """Extract the step from the runner options."""
    args = options.args
    main_class = options.main_class
    script_or_jar = options.script_or_jar

    if script_or_jar.lower().endswith('.jar'):
        return SparkJarStep(args=args, jar=script_or_jar,
                            main_class=main_class)
    elif script_or_jar.lower().endswith('.py'):
        return SparkScriptStep(args=args, script=script_or_jar)
    else:
        raise ValueError('%s appears not to be a JAR or Python script' %
                         options.script_or_jar)


def _get_spark_args(parser, cl_args):
    raw_args = _parse_raw_args(parser, cl_args)

    spark_args = []

    for dest, option_string, args in raw_args:
        if dest in _SPARK_ARG_OPT_NAMES:
            spark_args.append(option_string)
            spark_args.extend(args)

    return spark_args


def _add_spark_submit_arg(parser, opt_name):
    opt_string = _SPARK_SUBMIT_SWITCHES[opt_name]

    kwargs = dict(dest=opt_name)

    # if opt_name is a mrjob opt, parse args like a MRJob would
    if opt_name in _RUNNER_OPTS:
        opt_alias = _SWITCH_ALIASES.get(opt_string, opt_string)

        for opt_strings, opt_kwargs in _RUNNER_OPTS[opt_name]['switches']:
            if opt_alias in opt_strings:
                kwargs.update(opt_kwargs)

    parser.add_argument(opt_string, **kwargs)


def _make_arg_parser():
    # this parser is never used for help messages, so ordering,
    # usage, etc. don't matter
    parser = ArgumentParser()#add_help=False)

    # add positional arguments
    parser.add_argument(dest='script_or_jar')
    parser.add_argument(dest='args', nargs='*')

    _add_basic_args(parser)
    _add_runner_alias_arg(parser)
    #_add_help_arg(parser)

    # add runner opts
    runner_opt_names = (
        set(_RUNNER_OPTS) - set(_HARD_CODED_OPTS) - _IRRELEVANT_OPT_NAMES)
    _add_runner_args(parser, runner_opt_names)

    # add spark-specific opts (without colliding with runner opts)
    for opt_name, switch in _SPARK_SUBMIT_SWITCHES.items():
        if opt_name in runner_opt_names and switch not in _SWITCH_ALIASES:
            continue
        _add_spark_submit_arg(parser, opt_name)

    return parser


def _add_runner_alias_arg(parser):
    parser.add_argument(
        '-r', '--runner', dest='runner',
        default=_DEFAULT_RUNNER,
        choices=_SPARK_RUNNERS,
        help=('Where to run the job (default: %(default)s")'))


def _add_help_arg(parser):
    parser.add_argument(
        '-h', '--help', dest='help', action='store_true',
        help='show this message and exit')


def _make_basic_help_arg_parser():
    parser = ArgumentParser(usage=_USAGE, description=_DESCRIPTION)

    parser.add_argument(dest='script_or_jar')

    parser.add_argument(dest='args', nargs='*')

    _add_runner_alias_arg(parser)

    for group_desc, opt_names in _SPARK_SUBMIT_ARG_GROUPS:
        if group_desc is None:
            parser_or_group = parser
        else:
            parser_or_group = parser.add_argument_group(group_desc)

        for opt_name in opt_names:
            _add_spark_submit_arg(parser_or_group, opt_name)

        if group_desc is None:
            _add_basic_args(parser)
            # TODO: add --deprecated and --help

    return parser




if __name__ == '__main__':
    main()
