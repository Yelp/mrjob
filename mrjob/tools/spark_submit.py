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

from mrjob.job import MRJob
from mrjob.options import _RUNNER_OPTS
from mrjob.options import _add_basic_args
from mrjob.runner import _runner_class

_USAGE = ('%(prog)s spark-submit [-r <runner>] [options]'
          ' <python file | app jar> [app arguments]')

_DESCRIPTION = 'Submit a spark job using mrjob runners'

# for spark-submit args, just need switches and help message
# (which can be patched into runner opts with same dest name)

# then add runner opts (other than check_input_paths) but don't
# display in default help message



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


_SWITCH_ALIASES = {
    '--master': '--spark-master',
    '--deploy-mode': '--spark-deploy-mode',
    '--jars': '--libjars',
    '--conf': '--jobconf',
}



_CORE_OPTS = {


}


def main(cl_args=None):
    parser = _make_arg_parser()
    options = parser.parse_args(cl_args)

    print(options)

    MRJob.set_up_logging(
        quiet=options.quiet,
        verbose=options.verbose,
    )

    runner_class = _runner_class(options.runner)
    runner_kwargs = _get_runner_kwargs(options)

    runner = runner_class(**runner_kwargs)

    runner.run()


def _add_spark_submit_arg(opt_name, parser_or_group):
    opt_string = _SPARK_SUBMIT_SWITCHES[opt_name]

    kwargs = dict(dest=opt_name)

    # if opt_name is a mrjob opt, parse args like a MRJob would
    if opt_name in _RUNNER_OPTS:
        opt_alias = _SWITCH_ALIASES.get(opt_string, opt_string)

        for opt_strings, opt_kwargs in _RUNNER_OPTS[opt_name]['switches']:
            if opt_alias in opt_strings:
                kwargs.update(opt_kwargs)

    parser_or_group.add_argument(opt_string, **kwargs)


def _make_arg_parser():
    parser = ArgumentParser(usage=_USAGE, description=_DESCRIPTION)

    parser.add_argument(
        '-r', '--runner', dest='runner', default='hadoop',
        choices=('hadoop', 'emr', 'dataproc'),
        help=('Where to run the job (default: %(default)s")'))

    for group_desc, opt_names in _SPARK_SUBMIT_ARG_GROUPS:
        if group_desc is None:
            parser_or_group = parser
        else:
            parser_or_group = parser.add_argument_group(group_desc)

        for opt_name in opt_names:
            _add_spark_submit_arg(opt_name, parser_or_group)

        if group_desc is None:
            _add_basic_args(parser)
            # TODO: add --deprecated and --help

    return parser




if __name__ == '__main__':
    main()
