# Copyright 2019 Yelp
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
from tests.sandbox import BasicTestCase

from mrjob.logs.ids import _sort_for_spark

# _sort_by_recency() is tested implicitly by
# tests.logs.test_task.InterpretTaskLogsTestCase

class SortForSparkTestCase(BasicTestCase):

    def test_empty(self):
        self.assertEqual(_sort_for_spark([]), [])

    def test_sort_by_container_id(self):
        # this is actually what happens when you run a small Spark job
        # on EMR

        # should prioritize latest attempts (02) of lowest container

        self.assertEqual(
            _sort_for_spark([
                dict(container_id='container_1567111600525_0001_01_000001'),
                dict(container_id='container_1567111600525_0001_01_000002'),
                dict(container_id='container_1567111600525_0001_02_000001'),
            ]),
            [
                dict(container_id='container_1567111600525_0001_02_000001'),
                dict(container_id='container_1567111600525_0001_01_000001'),
                dict(container_id='container_1567111600525_0001_01_000002'),
            ],
        )

    def test_sort_by_timestamp_and_step_num(self):
        # this is just in case we have to sort logs from multiple steps
        self.assertEqual(
            _sort_for_spark([
                dict(container_id='container_1567111600524_0001_01_000001'),
                dict(container_id='container_1567111600525_0001_01_000002'),
                dict(container_id='container_1567111600525_0002_02_000001'),
            ]),
            [
                dict(container_id='container_1567111600525_0002_02_000001'),
                dict(container_id='container_1567111600525_0001_01_000002'),
                dict(container_id='container_1567111600524_0001_01_000001'),
            ],
        )
