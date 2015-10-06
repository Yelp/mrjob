# Copyright 2011 Yelp
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
"""
mr_postfix_bounce is a mrjob that parses a Postfix log file looking for
messages that have bounced and yielding the (email address, date ordinal).
The emitted email addresses can then be unconfirmed or handled in some other
way.
"""
import datetime
import json
import re
import time

from mrjob.job import MRJob

__author__ = 'Adam Derewecki <derewecki@gmail.com>'

PROCESS_TYPE_PATTERN = re.compile(
    r'postfix-(?P<queue>[^/]+)/(?P<process>[^[]+)\[\d+\]:')
MESSAGE_ID_PATTERN = re.compile(
    r'^(?P<message_id>[A-Z0-9]+): (?P<postfix_message>.*)')
VAR_PATTERN = re.compile(r'(?P<name>\w+)=(?P<value>[^ ,]+)')
HOST_PATTERN = re.compile(
    r'(?P<before>.*?)[\(]host (?P<host>\S+) (?P<action>[^:]+):'
    r' (?P<message>.*)[\)]')
KEY_VALUE_PATTERN = re.compile(r'(?:^|, )(?P<key>\w+)=(?P<value>[^, ]+)')
DOMAIN_PATTERN = re.compile(r'(?<=@)[^.]+\.\w+')


def process_log_line(line):
    # log lines don't have year, so make that up
    # Note: not safe over year transitions
    (date_year, date_month, date_day, date_time, host, process,
     postfix_message) = [str(datetime.date.today().year)] + line.split(None, 5)

    timetuple = time.strptime(
        ' '.join((date_month, date_day, date_time, date_year)),
        '%b %d %H:%M:%S %Y')
    timestamp = time.mktime(timetuple)
    date_ordinal = datetime.date(*timetuple[:3]).toordinal()

    process_type_match = PROCESS_TYPE_PATTERN.search(process)
    message_id_match = MESSAGE_ID_PATTERN.search(postfix_message)

    if process_type_match and message_id_match:
        if process_type_match.group('process') == 'smtp':
            after_message_id = message_id_match.group('postfix_message')
            postfix_log_dict = {}

            # match all key=value pairs but not ones in the (message) afterward
            # sometimes there isn't a (message)
            if '(' in after_message_id:
                # split on ( and grab first element in tuple
                key_value_section = after_message_id.split('(')[0]
            else:
                key_value_section = after_message_id

            postfix_log_dict = dict(
                KEY_VALUE_PATTERN.findall(key_value_section))
            # find where key=value ends and save the rest of the string
            if postfix_log_dict:
                after_vars_idx = max(
                    after_message_id.index(value) + len(value) + 1
                    for value in postfix_log_dict)
                after_vars = after_message_id[after_vars_idx:]
            else:
                after_vars = after_message_id

            host_match = HOST_PATTERN.search(after_vars)
            if host_match:
                postfix_log_dict.update({
                    'remote_smtp_string': host_match.group('message'),
                    'remote_smtp_string_type': host_match.group('action'),
                    'remote_host': host_match.group('host')
                })
            elif len(after_vars.strip()) > 0:
                postfix_log_dict['smtp_string'] = after_vars.strip()

            postfix_log_dict.update({
                'time': timestamp,
                'date_ordinal': date_ordinal,
                'message_id': message_id_match.group('message_id'),
                'queue': process_type_match.group('queue'),
                'process': process_type_match.group('process')
            })

            try:
                if 'to' in postfix_log_dict:
                    postfix_log_dict['domain'] = (
                        DOMAIN_PATTERN.search(
                            postfix_log_dict['to']).group().lower())
            except:
                pass
            return postfix_log_dict


def domain_startswith(postfix_log_dict, needle):
    return postfix_log_dict.get('domain').startswith(needle)


def process_postfix_log_dict(decoded, bounce_rules):
    if decoded and 'to' in decoded and decoded.get('status') == 'bounced':
        to = decoded.get('to', '').strip('<>')
        # check to see if Postfix couldn't deliver the message
        if decoded.get('dsn') == '5.4.4':
            if 'Host not found' in decoded.get('smtp_string'):
                return to

        # run over our per-domain bounce processing error conditions
        for domain_prefixes, failure_conditions in bounce_rules:
            if any(domain_startswith(decoded, domain)
                   for domain in domain_prefixes):
                for point_of_failure, failure_strings in (
                    failure_conditions.iteritems()):
                    for failure_string in failure_strings:
                        if failure_string in decoded.get(point_of_failure, ''):
                            return to


class MRPostfixBounce(MRJob):
    def configure_options(self):
        super(MRPostfixBounce, self).configure_options()
        self.add_file_option(
            '--bounce-processing-rules',
            dest='bounce_processing_rules',
            default='bounce_processing_rules.json',
            help='JSON file of bounce processing rules.'
        )

    def load_options(self, args):
        super(MRPostfixBounce, self).load_options(args=args)
        if self.is_task():
            with open(self.options.bounce_processing_rules) as bounce_rules_f:
                self.bounce_processing_rules = json.load(bounce_rules_f)

    def mapper(self, _, line):
        postfix_log_dict = process_log_line(line)
        if postfix_log_dict:
            email_address = process_postfix_log_dict(
                postfix_log_dict, self.bounce_processing_rules)
            if email_address:
                yield email_address, postfix_log_dict['date_ordinal']

    def reducer(self, email_address, dateordinals):
        yield email_address, tuple(dateordinals)


if __name__ == '__main__':
    MRPostfixBounce().run()
