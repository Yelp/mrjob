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
        'hadoop_bin',
        'project_id',  # used by GCS filesystem
        'region',  # used by S3 filesystem
        's3_endpoint',
        'spark_deploy_mode',
        'spark_master',
    }

    # everything except Hadoop JARs
    # streaming jobs are run using mrjob_spark_harness.py (in this directory)
    _STEP_TYPES = {
        'spark', 'spark_jar', 'spark_script', 'streaming',
    }
