import os

from mrjob.job import MRJob
from mrjob.protocol import RawProtocol

class MREnviron(MRJob):

    OUTPUT_PROTOCOL = RawProtocol

    def mapper(self, _, value):
        yield None, None

    def reducer(self, key, values):
        for k, v in sorted(os.environ.items()):
            yield k, v


if __name__ == '__main__':
    MREnviron.run()
