# Copyright 2019 Yelp and Google, Inc.
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
"""A runner that can run jobs on Spark, with or without Hadoop."""
from mrjob.bin import MRJobBinRunner

from mrjob.fs.composite import CompositeFilesystem
from mrjob.fs.gcs import GCSFilesystem
from mrjob.fs.gcs import google as google_libs_installed
from mrjob.fs.hadoop import HadoopFilesystem
from mrjob.fs.local import LocalFilesystem
from mrjob.fs.s3 import S3Filesystem
from mrjob.fs.s3 import boto3 as boto3_installed
from mrjob.fs.s3 import _is_permanent_boto3_error


class SparkJobRunner(MRJobBinRunner):
    """Runs a :py:class:`~mrjob.job.MRJob` on your Spark cluster (with or
    without Hadoop). Invoked when you run your job with ``-r spark``.
    """
    alias = 'spark'

    # other than ``spark_*``, these options are only used for filesystems
    OPT_NAMES = MRJobBinRunner.OPT_NAMES | {
        'aws_access_key_id',
        'aws_secret_access_key',
        'aws_session_token',
        'cloud_fs_sync_secs',
        'cloud_part_size_mb',
        'cloud_tmp_dir',
        'google_project_id',  # used by GCS filesystem
        'hadoop_bin',
        's3_endpoint',
        's3_region',  # only used along with s3_endpoint
        'spark_deploy_mode',
        'spark_master',
    }

    # everything except Hadoop JARs
    # streaming jobs are run using mrjob_spark_harness.py (in this directory)
    _STEP_TYPES = {
        'spark', 'spark_jar', 'spark_script', 'streaming',
    }

    def fs(self):
        # Spark supports basically every filesystem there is

        if not self._fs:
            self._fs = CompositeFilesystem()

            if boto3_installed:
                self._fs.add_fs('s3', S3Filesystem(
                    aws_access_key_id=self._opts['aws_access_key_id'],
                    aws_secret_access_key=self._opts['aws_secret_access_key'],
                    aws_session_token=self._opts['aws_session_token'],
                    s3_endpoint=self._opts['s3_endpoint'],
                    s3_region=self._opts['s3_region'],
                ), disable_if=_is_permanent_boto3_error)

            # TODO: break out credentials managing code
            if google_libs_installed and False:
                self._fs.add_fs('gcs', GCSFilesystem(  # fill in
                    ))

            self._fs.add_fs('hadoop', HadoopFilesystem(
                self._opts['hadoop_bin']))

            self._fs.add_fs('local', LocalFilesystem())

        return self._fs
