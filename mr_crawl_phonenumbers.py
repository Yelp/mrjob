# -*- coding: utf-8 -*-
import itertools
import re
import urlparse
 
import boto
import warc
 
from boto.s3.key import Key
from gzipstream import GzipStreamFile
from mrjob.job import MRJob
from mrjob.protocol import RawProtocol
 
 
PHONE_SEP = r"[\-. ()+]"
US_PHONE = {
    "prefix": r"[\D\b](1?",
    "first_digit": "[2-9]",
    "digits": [2, 3, 4],
    "suffix": r")[\D\b]"
}
 
 
def standardize_phone_number(number):
    number_sep = sep_re.split(number)
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
 
 
def make_phone_format(phone_format=US_PHONE):
    format_list = [phone_format["prefix"]]
    format_list += [phone_format["first_digit"]]
    for digit in phone_format["digits"][:-1]:
        format_list += [r"\d{", str(digit), "}", PHONE_SEP, r"+"]
    format_list += [r"\d{", str(phone_format["digits"][-1]), "}"]
    format_list += [phone_format["suffix"]]
    return r"".join(format_list)
 
 
us_phone_regex = re.compile(make_phone_format())
sep_re = re.compile(PHONE_SEP)
 
 
class MRExtractPhoneNumbers(MRJob):
    HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.NLineInputFormat'
    INPUT_PROTOCOL = RawProtocol
 
    def map_commoncrawl(self, s3_object):
        f = warc.WARCFile(fileobj=GzipStreamFile(s3_object))
        for record in f:
            for key, value in self.process_record(record):
                yield key, value
 
    def process_record(self, record):
        if record['content-type'] != 'text/plain':
            return
 
        payload = record.payload.read()
        for phone_number in us_phone_regex.findall(payload):
            yield standardize_phone_number(phone_number), {'url': record.header['warc-target-uri']}
 
    def fetch_yelp_business_data(self, s3_object):
        # Fake example data, for illustration
        yield '+14159083801', {'biz_id': 1000}
        yield '+14155550100', {'biz_id': 1001}
        yield '+14155550101', {'biz_id': 1002}
 
    def map_to_phone_number(self, _, s3_url):
        s3_url_parsed = urlparse.urlparse(s3_url)
        bucket_name = s3_url_parsed.netloc
        key_path = s3_url_parsed.path[1:]
 
        conn = boto.connect_s3()
        bucket = conn.get_bucket(bucket_name, validate=False)
        key = Key(bucket, key_path)
 
        if key_path.startswith('common-crawl'):
            # Assume it's a commoncrawl object
            for phone_number, data in self.map_commoncrawl(key):
                yield phone_number, data
        else:
            for phone_number, data in self.fetch_yelp_business_data(key):
                yield phone_number, data
 
    def reduce_by_phone_number(self, phone_number, values):
        biz_ids = []
        urls = []
        for each_value in values:
            if 'biz_id' in each_value:
                biz_ids.append(each_value['biz_id'])
            if 'url' in each_value:
                urls.append(each_value['url'])
 
            if len(biz_ids) > 1000 or len(urls) > 1000:
                break
 
        if not urls or not biz_ids:
            return
 
        # multiple businesses share same phone number, treat them as one id
        biz_id = ','.join(sorted(biz_ids))
        for each_url in urls:
            yield biz_id, each_url
 
    def map_to_domain(self, biz_id, url):
        domain = urlparse.urlparse(url).netloc.split(':')[0]
        if domain:
            yield domain, (url, biz_id)
 
    def filter_large_domains(self, domain, url_biz_id_pairs):
        # Filter out any domain with > 10 businesses
        first_pairs = list(itertools.islice(url_biz_id_pairs, 11))
        if len(first_pairs) > 10:
            yield 'filtered', domain
            return
 
        for url, biz_ids in first_pairs:
            yield biz_ids, url
 
    def steps(self):
        return [self.mr(mapper=self.map_to_phone_number, reducer=self.reduce_by_phone_number),
                self.mr(mapper=self.map_to_domain, reducer=self.filter_large_domains)]
 
 
if __name__ == '__main__':
    MRExtractPhoneNumbers.run()