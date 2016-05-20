# Copyright 2016 Yelp
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
"""terminate_idle_job_flows has been renamed to terminate_idle_clusters; this
is just a stub that runs terminate_idle_clusters with a deprecation warning."""
from __future__ import print_function
from sys import stderr

from .terminate_idle_clusters import main as real_main


def main(args=None):
    print(
        'terminate-idle-job-flows is a deprecated alias for'
        ' terminate-idle-clusters and will be removed in v0.6.0',
        file=stderr)
    real_main(args)


if __name__ == '__main__':
    main()
