"""A simple example of linking a hadoop jar with a Python step.

This example only works out-of-the-box on EMR; to make it work on Hadoop,
change HADOOP_EXAMPLES_JAR to the (local) path of your hadoop-examples.jar.

This also only works on a single input path/directory, due to limitations
of the example jar.
"""
from mrjob.job import MRJob
from mrjob.protocol import RawProtocol
from mrjob.step import JarStep

# use the file:// trick to access a jar hosted on the EMR machines
HADOOP_EXAMPLES_JAR = 'file:///home/hadoop/hadoop-examples.jar'


class MRJarStepExample(MRJob):
    """A contrived example that runs wordcount from the hadoop example
    jar, and then does a frequency count of the frequencies."""

    def steps(self):
        return [
            JarStep(jar=HADOOP_EXAMPLES_JAR, name='',
                    step_args=['wordcount', JarStep.INPUT, JarStep.OUTPUT]),
            self.mr(mapper=self.mapper, combiner=self.reducer,
                    reducer=self.reducer)
        ]

    def mapper(self, key, freq):
        yield int(freq), 1

    def reducer(self, freq, counts):
        yield freq, sum(counts)

    def pick_protocols(self, step_num, step_type):
        """Use RawProtocol to read output from the jar."""
        read, write = super(MRJarStepExample, self).pick_protocols(
            step_num, step_type)
        if (step_num, step_type) == (1, 'mapper'):
            read = RawProtocol().read

        return read, write


if __name__ == '__main__':
    MRJarStepExample.run()
