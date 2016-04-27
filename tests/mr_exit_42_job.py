"""Job that exits with return code 42, without creating a traceback"""
import os

from mrjob.job import MRJob


class MRExit42Job(MRJob):

    def mapper_final(self):
        os._exit(42)


if __name__ == '__main__':
    MRExit42Job.run()
