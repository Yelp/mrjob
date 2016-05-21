# Copyright 2013 David Marin
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
import os
import os.path

from mrjob.job import MRJob


class MROSWalkJob(MRJob):
    """Recursively return the name and size of each file in the current dir."""

    def mapper_final(self):
        # hook for test_local.LocalRunnerSetupTestCase.test_python_archive()
        try:
            import foo
            foo  # quiet pyflakes warning
        except ImportError:
            pass

        for dirpath, _, filenames in os.walk('.'):
            for filename in filenames:
                path = os.path.join(dirpath, filename)
                size = os.path.getsize(path)
                yield path, size


if __name__ == '__main__':
    MROSWalkJob.run()
