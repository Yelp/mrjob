# Copyright 2011 Peter Harrington
# Copyright 2013 David Marin
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
'''
MapReduce version of Pegasos SVM, using mrjob to automate job flow
More information on Pegasos can be found here:
http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.161.9629&rep=rep1&type=pdf

usage: python mr_pegasos_svm.py < kickStart.txt
where: kickStart.txt contains simulated outputs from the reducer

A pickled matrix of the data set is assumed to reside on disk
in the file svmDat.txt.  The last column of this matrix is assumed
to be the class labels: i.e. -1 or 1.

Pegasos does not explicitly solve for a bias term b like the SMO
algorithm.  To account for this you can add a 0th column of all 1s
to your data set -if you wish.

For questions or comments you can contact me at:
peter.b.harrington@gmail.com
'''
import pickle
from numpy import mat, zeros, shape, random

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from mrjob.step import MRStep


class MRsvm(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def __init__(self, *args, **kwargs):
        super(MRsvm, self).__init__(*args, **kwargs)
        self.data = pickle.load(open('<path to pickled matrix>/svmDat.txt'))
        self.w = []                     #weights vector
        self.eta = 0.69                 #learning rate
        self.dataList = []              #list of data points to evaluate
        self.k = self.options.batchsize #number of data points to evaluate
        self.numMappers = 1             #number of mappers
        self.t = 1                      #iteration number

    def configure_options(self):
        super(MRsvm, self).configure_options()
        self.add_passthrough_option(
            '--iterations', dest='iterations', default=2, type='int',
            help='T: number of iterations to run')
        self.add_passthrough_option(
            '--batchsize', dest='batchsize', default=100, type='int',
            help='k: number of data points in a batch')

    def map(self, mapperId, inVals): #needs exactly 2 arguments
        #input: mapperId, ['w', w-vector] OR mapperId, ['x', int]
        #output: none
        if False: yield
        if inVals[0]=='w':                  #accumulate W-vector
            self.w = inVals[1]
        elif inVals[0]=='x':
            self.dataList.append(inVals[1]) #accumulate data points to calc
        elif inVals[0]=='t': self.t = inVals[1]

    def map_fin(self):
        #input: none
        #output: 0, ['w', w-vector] OR 0, ['u', int] OR 0, ['t', int]
        labels = self.data[:,-1]; X=self.data[:,0:-1]#reshape data into X and Y
        for index in self.dataList:
            p = mat(self.w)*X[index,:].T    #calc p=w*dataSet[key].T
            if labels[index]*p < 1.0:       #check to see if this index is correctly classified
                yield (0, ['u', index])     #make sure everything has the same key
        yield (0, ['w', self.w])            #so it ends up at the same reducer
        yield (0, ['t', self.t])

    def reduce(self, _, packedVals):
        #input: 0, ['w', w-vector] OR 0, ['u', int] OR 0, ['t', int]
        #output: mapperId, ['w', w-vector] OR mapperId, ['x', int] OR mapperId, ['t', int]
        for valArr in packedVals:                       #unpack inputs
            if valArr[0]=='u':  self.dataList.append(valArr[1])
            elif valArr[0]=='w': self.w = valArr[1]
            elif valArr[0]=='t':  self.t = valArr[1]
        labels = self.data[:,-1]; X=self.data[:,0:-1]
        wMat = mat(self.w);   wDelta = mat(zeros(len(self.w)))
        for index in self.dataList:                     #accumulate changes to w
            wDelta += float(labels[index])*X[index,:]   #wDelta += label*dataSet
        eta = 1.0/(2.0*self.t)
        wMat = (1.0 - 1.0/self.t)*wMat + (eta/self.k)*wDelta #update w
        for mapperNum in range(self.numMappers):
            yield (mapperNum, ['w', wMat.tolist()[0] ]) #emit w
            if self.t < self.options.iterations:        #if this is not the last iteration
                yield (mapperNum, ['t', self.t+1])      #increment t
                for j in range(self.k/self.numMappers): #emit random ints for mappers iid
                    yield (mapperNum, ['x', random.randint(shape(self.data)[0]) ])

    def steps(self):
        return ([MRStep(mapper=self.map, reducer=self.reduce,
                        mapper_final=self.map_fin)]*self.options.iterations)


if __name__ == '__main__':
    MRsvm.run()
