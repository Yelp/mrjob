import mrjob
import os
from mrjob.job import MRJob


class MRJobWhereAreYou(MRJob):
    """Output what directory the mrjob library is in."""

    def mapper_final(self):
        yield (None, None)

    def reducer(self, key, values):
        yield (None, os.path.dirname(os.path.realpath(mrjob.__file__)))


if __name__ == '__main__':
    MRJobWhereAreYou.run()
