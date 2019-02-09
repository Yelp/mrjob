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
import logging
import os.path
import posixpath
from subprocess import CalledProcessError
from tempfile import gettempdir

from mrjob.bin import MRJobBinRunner
from mrjob.conf import combine_dicts
from mrjob.fs.composite import CompositeFilesystem
from mrjob.fs.gcs import GCSFilesystem
from mrjob.fs.gcs import google as google_libs_installed
from mrjob.fs.gcs import _is_permanent_google_error
from mrjob.fs.hadoop import HadoopFilesystem
from mrjob.fs.local import LocalFilesystem
from mrjob.fs.s3 import S3Filesystem
from mrjob.fs.s3 import boto3 as boto3_installed
from mrjob.fs.s3 import _is_permanent_boto3_error
from mrjob.hadoop import fully_qualify_hdfs_path
from mrjob.logs.step import _log_log4j_record
from mrjob.parse import is_uri
from mrjob.setup import UploadDirManager
from mrjob.step import StepFailedException

log = logging.getLogger(__name__)


class SparkMRJobRunner(MRJobBinRunner):
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
        'google_project_id',  # used by GCS filesystem
        'hadoop_bin',
        's3_endpoint',
        's3_region',  # only used along with s3_endpoint
        'spark_deploy_mode',
        'spark_master',
        'spark_tmp_dir',  # where to put temp files in Spark
    }

    # everything except Hadoop JARs
    # streaming jobs will be run using mrjob_spark_harness.py (see #1972)
    _STEP_TYPES = {
        'spark', 'spark_jar', 'spark_script', # 'streaming',
    }

    def __init__(self, **kwargs):
        super(SparkMRJobRunner, self).__init__(**kwargs)

        self._spark_tmp_dir = self._pick_spark_tmp_dir()

        # where local files are uploaded into Spark
        if is_uri(self._spark_tmp_dir):
            spark_files_dir = posixpath.join(self._spark_tmp_dir, 'files', '')
            self._upload_mgr = UploadDirManager(spark_files_dir)

        # where to put job output (if not set explicitly)
        if not self._output_dir:
            self._output_dir = posixpath.join(self._spark_tmp_dir, 'output')

        # keep track of where the spark-submit binary is
        self._spark_submit_bin = self._opts['spark_submit_bin']

    def _default_opts(self):
        return combine_dicts(
            super(SparkMRJobRunner, self)._default_opts(),
            dict(
                spark_master='local[*]',
                spark_deploy_mode='client',
            )
        )

    def _run(self):
        self.get_spark_submit_bin()  # find spark-submit up front
        self._create_setup_wrapper_scripts()
        self._add_job_files_for_upload()
        self._upload_local_files()
        self._run_steps_on_spark()

    def _add_job_files_for_upload(self):
        """Add files needed for running the job (setup and input)
        to self._upload_mgr."""
        if self._upload_mgr:
            for path in self._working_dir_mgr.paths():
                self._upload_mgr.add(path)

        # no need to upload py_files, spark-submit handles this

    def _pick_spark_tmp_dir(self):
        if self._opts['spark_tmp_dir']:
            if is_uri(self._opts['spark_tmp_dir']):
                return posixpath.join(
                    self._opts['spark_tmp_dir'], self._job_key)
            else:
                return os.path.join(
                    self._opts['spark_tmp_dir'], self._job_key)
        elif self._spark_master_is_local():
            # need a local temp dir
            # add "-spark" so we don't collide with default local temp dir
            return os.path.join(
                gettempdir(), self._job_key + '-spark')
        else:
            # use HDFS (same default as HadoopJobRunner)
            return posixpath.join(
                fully_qualify_hdfs_path('tmp/mrjob'), self._job_key)

    def _default_step_output_dir(self):
        return posixpath.join(self._spark_tmp_dir, 'step-output')

    @property
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

            if google_libs_installed:
                self._fs.add_fs('gcs', GCSFilesystem(
                    project_id=self._opts['google_project_id']
                ), disable_if=_is_permanent_google_error)

            self._fs.add_fs('hadoop', HadoopFilesystem(
                self._opts['hadoop_bin']))

            self._fs.add_fs('local', LocalFilesystem())

        return self._fs

    def _upload_local_files(self):
        # in local mode, nothing to upload
        if not self._upload_mgr:
            return

        self.fs.mkdir(self._upload_mgr.prefix)

        log.info('Copying local files to %s' % self._upload_mgr.prefix)
        for src_path, uri in self._upload_mgr.path_to_uri().items():
            log.debug('  %s -> %s' % (src_path, uri))
            self.fs.put(src_path, uri)

    def _run_steps_on_spark(self):
        for step_num, step in enumerate(self._get_steps()):
            self._run_step_on_spark(step, step_num)

    def _run_step_on_spark(self, step, step_num):
        if self._opts['upload_archives'] and self._spark_master() != 'yarn':
            log.warning('Spark master %r will probably ignore archives' %
                        self._spark_master())

        spark_submit_args = self._args_for_spark_step(step_num)

        env = dict(os.environ)
        env.update(self._spark_cmdenv(step_num))

        returncode = self._run_spark_submit(spark_submit_args, env,
                                            record_callback=_log_log4j_record)

        if returncode:
            reason = str(CalledProcessError(returncode, spark_submit_args))
            raise StepFailedException(
                reason=reason, step_num=step_num,
                num_steps=self._num_steps())
