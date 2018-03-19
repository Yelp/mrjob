# Copyright 2009-2016 Yelp and Contributors
# Copyright 2017 Yelp
# Copyright 2018 Google Inc.
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

# see case.py for definition of mock_clusters and mock_gcs_fs
from copy import deepcopy

from google.api_core.exceptions import InvalidArgument
from google.api_core.exceptions import NotFound
from google.cloud.dataproc_v1.types import Cluster
from google.cloud.dataproc_v1.types import ClusterStatus
from google.cloud.dataproc_v1.types import Job
from google.cloud.dataproc_v1.types import JobStatus


class MockGoogleDataprocClusterClient(object):

    """Mock out google.cloud.dataproc_v1.ClusterControllerClient"""
    def __init__(self, mock_clusters, mock_jobs, mock_gcs_fs):
        # maps (project_id, region, cluster_name) to a
        # google.cloud.dataproc_v1.types.Cluster
        self.mock_clusters = mock_clusters

        # maps (project_id, region, cluster_name, job_name) to a
        # google.cloud.dataproc_v1.types.Job
        self.mock_jobs = mock_jobs

        # see case.py
        self.mock_gcs_fs = mock_gcs_fs

    def create_cluster(self, project_id, region, cluster):
        # convert dict to object
        if not isinstance(cluster, Cluster):
            cluster = Cluster(**cluster)

        if cluster.project_id:
            if cluster.project_id != project_id:
                raise InvalidArgument(
                    'If provided, CreateClusterRequest.cluster.project_id must'
                    ' match CreateClusterRequest.project_id')
        else:
            cluster.project_id = project_id

        if not cluster.cluster_name:
            raise InvalidArgument('Cluster name is required')

        # initialize cluster status
        cluster.status.state = ClusterStatus.State.Value('CREATING')

        cluster_key = (project_id, region, cluster.cluster_name)

        # TODO: check conflict with existing cluster

        self.mock_clusters[cluster_key] = cluster

    def get_cluster(self, project_id, region, cluster_name):
        cluster_key = (project_id, region, cluster_name)
        if cluster_key not in self.mock_clusters:
            raise NotFound(
                'Not Found: Cluster'
                ' projects/%s/regions/%s/clusters/%s' %
                (project_id, region, cluster_name))

        cluster = self.mock_clusters[cluster_key]

        result = deepcopy(cluster)
        self._simulate_progress(cluster)
        return result

    def _simulate_progress(self, mock_cluster):
        # just move from STARTING to RUNNING
        mock_cluster.status.state = ClusterStatus.State.Value('RUNNING')


class MockGoogleDataprocJobClient(object):

    """Mock out google.cloud.dataproc_v1.JobControllerClient"""
    def __init__(self, mock_clusters, mock_jobs, mock_gcs_fs):
        self.mock_clusters = mock_clusters
        self.mock_jobs = mock_jobs
        self.mock_gcs_fs = mock_gcs_fs

    def submit_job(self, project_id, region, job):
        # convert dict to object
        if not isinstance(job, Job):
            job = Job(**job)

        if not (job.reference.project_id and job.reference.job_id):
            raise NotImplementedError('generation of job IDs not implemented')

        if not job.placement.cluster_name:
            raise InvalidArgument('Cluster name is required')

        if not job.hadoop_job:
            raise NotImplementedError('only hadoop jobs are supported')

        if job.reference.project_id:
            if job.reference.project_id != project_id:
                raise InvalidArgument(
                    'If provided, SubmitJobRequest.job.job_reference'
                    '.project_id must match SubmitJobRequest.project_id')
        else:
            job.reference.project_id = project_id

        # copy the job as submitted
        # TODO: look at what submit_job really returns
        result = deepcopy(job)

        job.status.state = JobStatus.State.Value('PENDING')

        cluster_key = (project_id, region, job.placement.cluster_name)
        cluster_jobs = self.mock_jobs.setdefault(cluster_key, {})

        # TODO: check for conflict

        cluster_jobs[job.reference.job_id] = job

        return result
