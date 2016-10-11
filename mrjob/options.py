# -*- coding: utf-8 -*-
# Copyright 2009-2016 Yelp and Contributors
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
"""Functions to populate py:class:`OptionParser` and :py:class:`OptionGroup`
objects with categorized command line parameters. This module should not be
made public until at least 0.4 if not later or never.
"""
import json
from optparse import OptionParser
from optparse import SUPPRESS_USAGE

from mrjob.parse import parse_key_value_list
from mrjob.parse import parse_port_range_list

from .options2 import CLEANUP_CHOICES
from .options2 import _CLEANUP_DEPRECATED_ALIASES
from .options2 import _add_runner_options
from .options2 import _allowed_keys
from .options2 import _combiners
from .options2 import _deprecated_aliases
from .options2 import _pick_runner_opts


def _append_to_conf_paths(option, opt_str, value, parser):
    """conf_paths is None by default, but --no-conf or --conf-path should make
    it a list.
    """

    if parser.values.conf_paths is None:
        parser.values.conf_paths = []

    # this method is also called during generate_passthrough_arguments
    # the check below is to ensure that conf_paths are not duplicated
    if value not in parser.values.conf_paths:
        parser.values.conf_paths.append(value)


def _add_basic_opts(opt_group):
    """Options for all command line tools"""

    opt_group.add_option(
        '-c', '--conf-path', dest='conf_paths', action='callback',
        callback=_append_to_conf_paths, default=None, nargs=1,
        type='string',
        help='Path to alternate mrjob.conf file to read from')

    opt_group.add_option(
        '--no-conf', dest='conf_paths', action='store_const', const=[],
        help="Don't load mrjob.conf even if it's available")

    opt_group.add_option(
        '-q', '--quiet', dest='quiet', default=None,
        action='store_true',
        help="Don't print anything to stderr")

    opt_group.add_option(
        '-v', '--verbose', dest='verbose', default=None,
        action='store_true', help='print more messages to stderr')


def _add_job_opts(opt_group):
    opt_group.add_option(
        '--no-output', dest='no_output',
        default=None, action='store_true',
        help="Don't stream output after job completion")

    opt_group.add_option(
        '-o', '--output-dir', dest='output_dir', default=None,
        help='Where to put final job output. This must be an s3:// URL ' +
        'for EMR, an HDFS path for Hadoop, and a system path for local,' +
        'and must be empty')

    opt_group.add_option(
        '--partitioner', dest='partitioner', default=None,
        help=('Hadoop partitioner class. Deprecated as of v0.5.1 and'
              ' will be removed in v0.6.0 (specify in your job instead)'))

    opt_group.add_option(
        '-r', '--runner', dest='runner', default=None,
        choices=('local', 'hadoop', 'emr', 'inline', 'dataproc'),
        help=('Where to run the job; one of dataproc, emr, hadoop, inline,'
              ' or local'))



def _print_help_for_groups(*args):
    option_parser = OptionParser(usage=SUPPRESS_USAGE, add_help_option=False)
    option_parser.option_groups = args
    option_parser.print_help()


def _alphabetize_options(opt_group):
    opt_group.option_list.sort(key=lambda opt: opt.dest or '')


# TODO: phase this out
def _fix_custom_options(options, option_parser):
    """Update *options* to handle KEY=VALUE options, etc."""

    if hasattr(options, 'emr_configurations'):
        decoded_configurations = []

        for c in options.emr_configurations:
            try:
                decoded_configurations.append(json.loads(c))
            except ValueError as e:
                option_parser.error(
                    'Malformed JSON passed to --emr-configuration: %s' % (
                        str(e)))

        options.emr_configurations = decoded_configurations

    if getattr(options, 'ssh_bind_ports', None):
        try:
            ports = parse_port_range_list(options.ssh_bind_ports)
        except ValueError as e:
            option_parser.error('invalid port range list %r: \n%s' %
                                (options.ssh_bind_ports, e.args[0]))

        options.ssh_bind_ports = ports
