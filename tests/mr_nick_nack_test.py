import json

from mrjob.job import MRJob
from mrjob.protocol import RawProtocol


class MRNickNackTest(MRJob):

  # tell mrjob not to format our output -- we're going to leave that to hadoop
  OUTPUT_PROTOCOL = RawProtocol

  # tell hadoop to massage our mrjob output using this output format
  HADOOP_OUTPUT_FORMAT = 'nicknack.MultipleValueOutputFormat'

  def mapper_init(self):
    yield "csv-output", '1,two,"3 four"'
    yield "json-output", json.dumps({"one":1, "two": "two"})

  def mapper(self, k, v):
      return


if __name__ == '__main__':
    MRNickNackTest.run()
