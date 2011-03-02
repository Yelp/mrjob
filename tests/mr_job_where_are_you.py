import mrjob
import os
from mrjob.job import MRJob


class MRJobWhereAreYou(MRJob):
    """very simple job that outputs the location of the mrjob library."""

    def mapper_final(self):
        yield (None, None)

    def reducer(self, key, values):
        yield (None, os.path.realpath(mrjob.__file__))


if __name__ == '__main__':
    MRJobWhereAreYou.run()
