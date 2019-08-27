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
from .ids import _add_implied_task_id
from .log4j import _parse_hadoop_log4j_records
from .wrap import _cat_log_lines


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

    for record in records:
        if record['level'] in ('WARN', 'ERROR'):
            error = dict(
                spark_error=dict(
                    message=record['message'],
                    start_line=record['start_line'],
                    num_lines=record['num_lines'],
                )
            )

            if not result.get('errors'):
                result['errors'] = []

            result['errors'].append(error)

    return result


def _interpret_spark_logs(fs, matches, partial=True, log_callback=None):
    result = {}
    errors = []

    matches = list(matches)

    for match in matches:
        path = match['path']
        if log_callback:
            log_callback(path)

        interpretation = _parse_spark_log(_cat_log_lines(fs, path))

        result.update(interpretation)
        # don't _add_implied_job_id() because it doesn't work that way on Spark

        for error in interpretation.get('errors') or ():
            if 'spark_error' in error:
                error['spark_error']['path'] = path

            for id_key in 'attempt_id', 'container_id':
                if id_key in match:
                    error[id_key] = match[id_key]
            _add_implied_task_id(error)

            errors.append(error)

        if partial and result.get('errors'):
            result['partial'] = True
            break

    result['errors'] = errors
    return result
