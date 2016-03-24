"""Simple example about what mapper_final() doesn't do.

Referenced from docs/guides/testing.rst
"""
from mrjob.job import MRJob

class MRCountLinesWrong(MRJob):

    def mapper_init(self):
        self.num_lines = 0

    def mapper(self, _, line):
        self.num_lines += 1

    def mapper_final(self):
        yield None, self.num_lines


if __name__ == '__main__':
    MRCountLinesWrong.run()
