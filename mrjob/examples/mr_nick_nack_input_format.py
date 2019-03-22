from mrjob.examples.mr_nick_nack import MRNickNack


class MRNickNackWithHadoopInputFormat(MRNickNack):

    HADOOP_INPUT_FORMAT = 'nicknack.ManifestTextInputFormat'


if __name__ == '__main__':
    MRNickNackWithHadoopInputFormat.run()
