"""Which files are we reading input from?"""
from mrjob.compat import jobconf_from_env
from mrjob.job import MRJob

class MRWhichFiles(MRJob):

    def mapper_init(self):
        yield jobconf_from_env('mapreduce.map.input.file'), None

    def mapper(self, _, line):
        pass  # disable identity mapper

    def reducer(self, path, _):
        yield None, path


if __name__ == '__main__':
    MRWhichFiles.run()
