# Copyright 2010-2012 Yelp
# Copyright 2013 David Marin and Steve Johnson
# Copyright 2015-2016 Yelp
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
"""Delete all files in a given URI that are older than a specified time.  The
time parameter defines the threshold for removing files. If the file has not
been accessed for *time*, the  file is removed. The time argument is a number
with an optional single-character suffix specifying the units: m for minutes,
h for hours, d for days.  If no suffix is specified, time is in hours.

Suggested usage: run this as a cron job with the -q option::

    0 0 * * * mrjob s3-tmpwatch -q 30d s3://your-bucket/tmp/

Usage::

    mrjob s3-tmpwatch [options] <time-untouched> <URIs>

Options::

  -h, --help            show this help message and exit
  -c CONF_PATHS, --conf-path=CONF_PATHS
                        Path to alternate mrjob.conf file to read from
  --no-conf             Don't load mrjob.conf even if it's available
  -q, --quiet           Don't print anything to stderr
  --region=REGION       GCE/AWS region to run Dataproc/EMR jobs in.
  --aws-region=REGION   Deprecated alias for --region
  --s3-endpoint=S3_ENDPOINT
                        Force mrjob to connect to S3 on this endpoint (e.g. s3
                        -us-west-1.amazonaws.com). You usually shouldn't set
                        this; by default mrjob will choose the correct
                        endpoint for each S3 bucket based on its location.
  -t, --test            Don't actually delete any files; just log that we
                        would
  -v, --verbose         print more messages to stderr
"""
from datetime import datetime
from datetime import timedelta
import logging
from optparse import OptionParser

from mrjob.emr import EMRJobRunner
from mrjob.emr import iso8601_to_datetime
from mrjob.job import MRJob
from mrjob.options import _add_basic_options
from mrjob.options import _add_runner_options
from mrjob.options import _alphabetize_options
from mrjob.parse import parse_s3_uri


log = logging.getLogger(__name__)


def main(cl_args=None):
    option_parser = _make_option_parser()
    options, args = option_parser.parse_args(cl_args)

    MRJob.set_up_logging(quiet=options.quiet, verbose=options.verbose)

    # make sure time and uris are given
    if not args or len(args) < 2:
        option_parser.error('Please specify time and one or more URIs')

    time_old = _process_time(args[0])

    for path in args[1:]:
        _s3_cleanup(path, time_old,
                    dry_run=options.test,
                    **_runner_kwargs(options))


def _s3_cleanup(glob_path, time_old, dry_run=False, **runner_kwargs):
    """Delete all files older than *time_old* in *path*.

    If *dry_run* is true, then just log the files that need to be
    deleted without actually deleting them
    """
    runner = EMRJobRunner(**runner_kwargs)

    log.info('Deleting all files in %s that are older than %s' %
             (glob_path, time_old))

    for path in runner.fs.ls(glob_path):
        bucket_name, key_name = parse_s3_uri(path)
        bucket = runner.fs.get_bucket(bucket_name)

        for key in bucket.list(key_name):
            last_modified = iso8601_to_datetime(key.last_modified)
            age = datetime.utcnow() - last_modified
            if age > time_old:
                # Delete it
                log.info('Deleting %s; is %s old' % (key.name, age))
                if not dry_run:
                    key.delete()


def _runner_kwargs(options):
    """Options to pass to the EMRJobRunner."""
    kwargs = options.__dict__.copy()
    for unused_arg in ('quiet', 'verbose', 'test'):
        del kwargs[unused_arg]

    return kwargs


def _process_time(time):
    if time[-1] == 'm':
        return timedelta(minutes=int(time[:-1]))
    elif time[-1] == 'h':
        return timedelta(hours=int(time[:-1]))
    elif time[-1] == 'd':
        return timedelta(days=int(time[:-1]))
    else:
        return timedelta(hours=int(time))


def _make_option_parser():
    usage = '%prog [options] <time-untouched> <URIs>'
    description = (
        'Delete all files in a given URI that are older than a specified'
        ' time.\n\nThe time parameter defines the threshold for removing'
        ' files. If the file has not been accessed for *time*, the file is'
        ' removed. The time argument is a number with an optional'
        ' single-character suffix specifying the units: m for minutes, h for'
        ' hours, d for days.  If no suffix is specified, time is in hours.')

    option_parser = OptionParser(usage=usage, description=description)

    option_parser.add_option(
        '-t', '--test', dest='test', default=False,
        action='store_true',
        help="Don't actually delete any files; just log that we would")

    _add_basic_options(option_parser)
    _add_runner_options(
        option_parser,
        set(['region', 's3_endpoint']),
    )

    _alphabetize_options(option_parser)

    return option_parser


if __name__ == '__main__':
    main()
