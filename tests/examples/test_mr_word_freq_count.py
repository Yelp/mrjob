from mrjob.examples.mr_word_freq_count import MRWordFreqCount

from tests.job import run_job
from tests.py2 import TestCase


class MRWordFreqCountTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(run_job(MRWordFreqCount()), {})

    def test_the_wheels_on_the_bus(self):
        RAW_INPUT = b"""
        The wheels on the bus go round and round,
        round and round, round and round
        The wheels on the bus go round and round,
        all through the town.
        """

        EXPECTED_OUTPUT = {
            u'all': 1,
            u'and': 4,
            u'bus': 2,
            u'go': 2,
            u'on': 2,
            u'round': 8,
            u'the': 5,
            u'through': 1,
            u'town': 1,
            u'wheels': 2,
        }

        self.assertEqual(run_job(MRWordFreqCount(), RAW_INPUT),
                         EXPECTED_OUTPUT)
