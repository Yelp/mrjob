# Copyright 2010-2011 Yelp
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
    0 0 * * * python -m mrjob.tools.emr.s3_tmpwatch -q 30d \
s3://your-bucket/tmp/

Usage::

    mrjob s3-tmpwatch [options] <time-untouched> <URIs>
    python -m mrjob.tools.emr.s3_tmpwatch [options] <time-untouched> <URIs>

Options::

  -h, --help            show this help message and exit
  -v, --verbose         Print more messages
  -q, --quiet           Report only fatal errors.
  -c CONF_PATH, --conf-path=CONF_PATH
                        Path to alternate mrjob.conf file to read from
  --no-conf             Don't load mrjob.conf even if it's available
  -t, --test            Don't actually delete any files; just log that we
                        would

"""
from datetime import datetime
from datetime import timedelta
import logging
from optparse import OptionParser

try:
    import boto.utils
    boto  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    boto = None

from mrjob.emr import EMRJobRunner
from mrjob.emr import iso8601_to_datetime
from mrjob.job import MRJob
from mrjob.options import add_basic_opts
from mrjob.parse import parse_s3_uri


log = logging.getLogger(__name__)


def main(cl_args=None):
    option_parser = make_option_parser()
    options, args = option_parser.parse_args(cl_args)

    MRJob.set_up_logging(quiet=options.quiet, verbose=options.verbose)

    # make sure time and uris are given
    if not args or len(args) < 2:
        option_parser.error('Please specify time and one or more URIs')

    time_old = process_time(args[0])

    for path in args[1:]:
        s3_cleanup(path, time_old,
                   conf_paths=options.conf_paths,
                   dry_run=options.test)


def s3_cleanup(glob_path, time_old, dry_run=False, conf_paths=None):
    """Delete all files older than *time_old* in *path*.
       If *dry_run* is ``True``, then just log the files that need to be
       deleted without actually deleting them
       """
    runner = EMRJobRunner(conf_paths=conf_paths)
    s3_conn = runner.make_s3_conn()

    log.info('Deleting all files in %s that are older than %s' %
             (glob_path, time_old))

    for path in runner.ls(glob_path):
        bucket_name, key_name = parse_s3_uri(path)
        bucket = s3_conn.get_bucket(bucket_name)

        for key in bucket.list(key_name):
            last_modified = iso8601_to_datetime(key.last_modified)
            age = datetime.utcnow() - last_modified
            if age > time_old:
                # Delete it
                log.info('Deleting %s; is %s old' % (key.name, age))
                if not dry_run:
                    key.delete()


def process_time(time):
    if time[-1] == 'm':
        return timedelta(minutes=int(time[:-1]))
    elif time[-1] == 'h':
        return timedelta(hours=int(time[:-1]))
    elif time[-1] == 'd':
        return timedelta(days=int(time[:-1]))
    else:
        return timedelta(hours=int(time))


def make_option_parser():
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

    add_basic_opts(option_parser)

    return option_parser


if __name__ == '__main__':
    main()
