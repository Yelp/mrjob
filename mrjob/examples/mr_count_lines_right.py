"""Simple example about what mapper_final() doesn't do.

Referenced from docs/guides/testing.rst
"""
from mrjob.job import MRJob

class MRCountLinesRight(MRJob):

    def mapper_init(self):
        self.num_lines = 0

    def mapper(self, _, line):
        self.num_lines += 1

    def mapper_final(self):
        yield None, self.num_lines

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRCountLinesRight.run()
