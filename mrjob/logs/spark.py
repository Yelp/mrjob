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
"""Parse Spark driver and executor output. This can appear as either a "step"
log (output of the spark-submit binary) or as a "task" log (executors in
YARN containers), but has more or less the same format in either case."""
from .log4j import _parse_hadoop_log4j_records

# if a message ends with this, it's the beginning of a traceback
_TRACEBACK_ENDS_WITH = 'Traceback (most recent call last):'

# if a traceback starts with this, strip it from the error message
_CAUSED_BY = 'Caused by: '


def _parse_spark_log(lines, record_callback=None):

    def yield_records():
        for record in _parse_hadoop_log4j_records(lines):
            if record_callback:
                record_callback(record)
            yield record

    return _parse_spark_log_from_log4j_records(yield_records())


def _parse_spark_log_from_log4j_records(records):

    # make sure *records* is a generator
    records = iter(records)

    result = {}

    def add_task_error(task_error):
        if not result.get('errors'):
            result['errors'] = []

        result['errors'].append(dict(task_error=task_error))

    for record in records:
        msg = record['message']

        # is it a Python traceback?
        if (msg.rstrip().endswith(_TRACEBACK_ENDS_WITH) and
                msg.lstrip() == msg):

            # strip "Caused by:", which is just confusing
            if msg.startswith(_CAUSED_BY):
                msg = msg[len(_CAUSED_BY):]

            task_error = dict(
                message=record['message'],
                start_line=record['start_line'],
                num_lines=record['num_lines'],
            )

            # non-log4j driver output, reconstruct the message
            if task_error['num_lines'] == 1:
                for record in records:
                    task_error['message'] += '\n' + record['message']
                    task_error['num_lines'] += record['num_lines']

                # last line of traceback is not indented
                if record['message'] == record['message'].lstrip():
                    break

            add_task_error(task_error)

    return result
