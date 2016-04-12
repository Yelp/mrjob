"""A job that always fails."""
from mrjob.job import MRJob

class MRBoom(MRJob):

    def mapper_init(self):
        raise Exception('BOOM')

if __name__ == '__main__':
    MRBoom.run()
