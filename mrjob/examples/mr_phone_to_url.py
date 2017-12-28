# -*- coding: utf-8 -*-
import re
from itertools import islice

from mrjob.job import MRJob
from mrjob.py2 import urlparse
from mrjob.step import MRStep

import warc


PHONE_SEP_RE = re.compile(r'[\-. ()+]')
US_PHONE_RE = re.compile(
    r'[\D\b](1?[2-9]\d{2}[\-. ()+]+\d{3}[\-. ()+]+\d{4})[\D\b]')

# hosts with more than this many phone numbers are assumed to be directories
MAX_PHONES_PER_HOST = 1000


def standardize_phone_number(number):
    number_sep = PHONE_SEP_RE.split(number)
    number = "".join(number_sep)
    if len(number) > 7:
        if number[-1] not in '0123456789':
            number = number[:-1]
        if number[0] not in '0123456789':
            number = number[1:]
    if len(number) <= 10:
        return "+1" + number
    else:
        return "+" + number


class MRPhoneToURL(MRJob):
    """Use Common Crawl .wet files to map from phone number to the most
    likely URL."""

    def steps(self):
        return [
            MRStep(mapper_init=self.extract_phone_and_url_mapper_init,
                   reducer=self.count_by_host_reducer),
            MRStep(reducer=self.pick_best_url_reducer),
        ]

    def extract_phone_and_url_mapper_init(self):
        """Read .wet from ``self.stdin`` (so we can read multi-line records).

        Yield ``host, (phone, url)``
        """
        wet_file = warc.WARCFile(fileobj=self.stdin)

        for record in wet_file:
            if record['content-type'] != 'text/plain':
                continue

            url = record.header['warc-target-uri']
            host = urlparse(url).netloc

            payload = record.payload.read()
            for phone in US_PHONE_RE.findall(payload):
                phone = standardize_phone_number(phone)
                yield host, (phone, url)

    def count_by_host_reducer(self, host, phone_urls):
        phone_urls = list(islice(phone_urls, MAX_PHONES_PER_HOST + 1))

        # don't bother with directories, etc.
        host_phone_count = len(phone_urls)
        if host_phone_count > MAX_PHONES_PER_HOST:
            return

        for phone, url in phone_urls:
            yield phone, (url, host_phone_count)

    def pick_best_url_reducer(self, phone, urls_with_count):
        # pick the url that appears on a host with the least number of
        # phone numbers, breaking ties by choosing the shortest URL
        # and the one that comes first alphabetically
        urls_with_count = sorted(
            urls_with_count, key=lambda uc: (-uc[1], -len(uc[0]), uc[0]))

        yield phone, urls_with_count[0][0]


if __name__ == '__main__':
    MRPhoneToURL.run()
