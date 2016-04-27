# Copyright 2013 Andrew Price
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
An example of using bash_wrap to run external scripts for the mapper and reducer.
The external script in this case is wordcount.sh, which is used for both the mapper
and reducer by specifying either 'mapper' or 'reducer' as an argument when it is called.

To test this example locally run the following command after replacing MRJOBCONF_FILE 
with the configuration file location, and INPUT_FILE with a text file to use as input 
to the job:
python BashWordcount.py -c MRJOBCONF_FILE -r local --file wordcount.sh INPUT_FILE

To test this example on EMR run the following command after replacing OUTPUT_PATH with 
the location on S3 where you want the output to be saved, MRJOBCONF_FILE with the 
configuration file location:
python BashWordcount.py -c MRJOBCONF_FILE -r emr --file wordcount.sh \
--no-output --output-dir 'OUTPUT_PATH' 's3://elasticmapreduce/samples/wordcount/input/*'
"""

__author__ = 'Andrew Price <andrew.price@ensighten.com>'

from mrjob.job import MRJob
from mrjob.util import bash_wrap

class BashWordcount(MRJob):

    def mapper_cmd(self):
        return bash_wrap('./wordcount.sh mapper')

    def reducer_cmd(self):
        return bash_wrap('./wordcount.sh reducer')

if __name__ == '__main__':
    BashWordcount.run()
