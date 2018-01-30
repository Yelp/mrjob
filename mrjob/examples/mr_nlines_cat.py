from mrjob.job import MRJob

class MRNLinesCat(MRJob):

    HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.NLineInputFormat'

    def mapper(self, key, value):
        yield key, value


if __name__ == '__main__':
    MRNLinesCat.run()
