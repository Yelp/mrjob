"""
1-step matrix multiplication
Reference :
    Mining of Massive Datasets - The Stanford University InfoLab
    Link http://i.stanford.edu/~ullman/mmds/book.pdf

The input is 2 matrices which are represented
in the sparse format [i,j] = cell_value
eg: 2 sample matrices

[0,1]	2
[1,0]	3
[1,1]	4
[0,0]	2

and

[0,1]	2
[1,0]	2
[0,0]	2

USAGE:
    python mr_matrix_multiplication.py sample_files/1.matrix sample_files/2.matrix -r local --m1-rowcount 2 \
    --m2-colcount 2 -m1-colcount 2 --matrix1 sample_files/1.matrix --matrix2 sample_files/2.matrix

Example Output(will also be a sparse matrix) :
[0, 0]	8
[0, 1]	4.0
[1, 0]	14
[1, 1]	6.0

"""
from optparse import OptionError
import os
from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol

class MRMatrixMultiplication(MRJob):

    INPUT_PROTOCOL = JSONProtocol
    def configure_options(self):
        super(MRMatrixMultiplication, self).configure_options()
        self.add_passthrough_option('--m1-rowcount', help="Row count of Matrix1")
        self.add_passthrough_option('--m1-colcount', help="Column count of Matrix1")
        self.add_passthrough_option('--m2-colcount', help="Column count of Matrix2")
        self.add_passthrough_option('--matrix1', help="Matrix1 Filename")
        self.add_passthrough_option('--matrix2', help="Matrix2 Filename")

    def load_options(self, args):
        super(MRMatrixMultiplication, self).load_options(args=args)
        if not self.options.m1_rowcount or not self.options.m2_colcount or not self.options.matrix1 \
           or not self.options.matrix2 or not self.options.m1_colcount:
            raise OptionError("All Options must be present")


    def cell_mapper(self, coordinates, value):
        """
        We have 2 matrices m1 and m2

        m1:
        for k in m2.columns
            emits [i,k],(m1, j, m1(i,j))
        m2:
        for k in m1.rows
            emit [i,k],(m2, j, m2(i,j))

        """
        i,j = coordinates
        filename = os.environ['map_input_file']

        if filename == self.options.matrix1:
            for k in xrange(int(self.options.m2_colcount)):
                yield [i,k], ['m1', j, value]
        elif filename == self.options.matrix2:
            for k in xrange(int(self.options.m1_rowcount)):
                yield [k,j], ['m2', i, value]



    def multiply_reducer(self, key, values):
        """
        The (i,k)th element in the product matrix is
        sum(m1(j)*m2(j))
        """
        product = 0
        # for huge rows/columns having both of the row/col vectors as a dict helps in reducing
        # time complexity to join both vectors(which now becomes O(k) from O(k^2));
        m1_vector = {}
        m2_vector = {}
        for vector in values:
            if vector[0] == 'm1':
                m1_vector[vector[1]] = vector[2]
            else:
                m2_vector[vector[1]] = vector[2]

        for k in xrange(int(self.options.m1_colcount)):
            product += (m1_vector.get(k, 0.0) * m2_vector.get(k, 0.0))
        yield key, product


    def steps(self):
        return [self.mr(mapper=self.cell_mapper,
            reducer=self.multiply_reducer
            )]

if __name__ == '__main__':
    MRMatrixMultiplication.run()
