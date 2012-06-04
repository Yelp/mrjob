"""Job that exits with return code 42, without creating a traceback"""
import os

from tests.mr_testing_job import MRTestingJob


class MRExit42Job(MRTestingJob):

    def mapper_final(self):
        os._exit(42)


if __name__ == '__main__':
    MRExit42Job.run()
