# Copyright 2011 Jordan Andersen
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

"""
A brute force Map/Reduce solution to the Travelling Salesman Problem. The
purpose of this example is to demonstrate how to use Map/Reduce on
computationally intense problems that involve a relatively small input.

See the Wikipedia article for details of the problem:
http://en.wikipedia.org/wiki/Travelling_salesman_problem

The solution works by having each mapper find the longest/shortest tour in a
chunk of the full range of the possible factorial(N-1) tours. (Where N is the
number of nodes in the graph). The reducers then pick the winners from each
mapper.
"""
__author__ = 'Jordan Andersen <jordandandersen@gmail.com>'

import json
import numpy
import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from scipy.misc.common import factorial


def map_int_to_tour(num_nodes, i, start_node):
    """Gets a unique tour through a graph given an integer and starting node.

    Args:
    num_nodes -- the number of nodes in the graph being toured
    i -- the integer to be mapped to the set of tours for the graph
    start_node -- the node index to begin and end the tour on
    """
    nodes_remaining = range(0, start_node) + range(start_node + 1, num_nodes)
    tour = []

    while len(nodes_remaining) > 0:
        num_nodes = len(nodes_remaining)
        next_step = nodes_remaining[i % num_nodes]
        nodes_remaining.remove(next_step)
        tour.append(next_step)
        i = i / num_nodes

    tour = [start_node] + tour + [start_node]
    return tour


def cost_tour(graph, tour):
    """Calculates the travel cost of given tour through a given graph.

    Args:
    graph -- A square numpy.matrix representing the travel cost of each edge on
            the graph.
    tour -- A list of integers representing a tour through the graph where each
            entry is the index of a node on the graph.
    """
    steps = zip(tour[0:-1], tour[1:])
    cost = sum([graph[step_from, step_to] for step_from, step_to in steps])
    return cost


class MRSalesman(MRJob):

    def steps(self):
        """Defines the two steps, which are as follows:

        1.  Mapper splits the problem into reasonable chunks by mapping each
            possible tour to the integers and assigning each Step 2 mapper a
            range of tours to cost.
        2.  The mapper takes a range of tours and a description of the trip and
            yields the longest and shortests tours. The reduces yields the
            longest of the long and the shortest of the short tours.

        Notice the first step has no reducer. This allows all of the keys put
        out by the first step to be inputs to step 2's mappers without having
        to be reduced.
        """
        return ([MRStep(mapper=self.splitter),
                 MRStep(mapper=self.mapper,
                        reducer=self.reducer,
                        mapper_final=self.mapper_final)]
                )

    def __init__(self, *args, **kwargs):
        """Initializes an instance of the MRSalesman class. See MRJob for
        arguments.

        Some instance variables are initialized here that will be modified
        with while mapping in step 2 and output but the step 2 mapper_final.
        """
        super(MRSalesman, self).__init__(*args, **kwargs)
        self.shortest_length = sys.maxsize
        self.shortest_path = []
        self.longest_length = 0
        self.longest_path = []

    def splitter(self, key, line):
        """The mapper for step 1. Splits the range of possible tours into
        reasonably sized chunks for the consumption of the step 2 mappers.

        At this point the 'line' input should come directly from the first line
        of the one-line json file contains the edge cost graph and the starting
        node. The key is not relevant.
        """
        #loading the json description of the trip to get at the size
        #of the edge costgraph
        sales_trip = json.loads(line)
        m = numpy.matrix(sales_trip['graph'])
        num_nodes = m.shape[0]
        num_tours = factorial(num_nodes - 1)

        #Here we break down the full range of possible tours into smaller
        #pieces. Each piece is passed along as a key along with the trip
        #description.
        step_size = int(100 if num_tours < 100**2 else num_tours / 100)
        steps = range(0, num_tours, step_size) + [num_tours]
        ranges = zip(steps[0:-1], steps[1:])

        for range_low, range_high in ranges:
            #The key prepresents the range of tours to cost
            yield( ("%d-%d"%(range_low,range_high), sales_trip ))

    def mapper(self, key, sales_trip):
        """Mapper for step 2. Finds the shortest and longest tours through a
        small range of all possible tours through the graph.

        At this step the key will contain a string describing the range of
        tours to cost. The sales_trip has the edge cost graph and the starting
        node in a dict.
        """
        #This first line makes this function a generator function rather than a
        #normal function, which MRJob requires in its mapper functions. You need
        #to do this when all the output comes from the mapper_final.
        if False: yield
        matrix = numpy.matrix(sales_trip['graph'])
        num_nodes = matrix.shape[0]

        #The key prepresents the range of tours to cost
        range_low, range_high = map(int,key.split('-'))
        for i in range(range_low,range_high):

            tour = map_int_to_tour(num_nodes, i, sales_trip['start_node'])
            cost = cost_tour(matrix, tour)

            if cost < self.shortest_length:
                self.shortest_length = cost
                self.shortest_path = tour

            if cost > self.longest_length:
                self.longest_length = cost
                self.longest_path = tour

    def mapper_final(self):
        """Mapper_final for step 2. Outputs winners found by mapper."""
        yield ('shortest', (self.shortest_length, self.shortest_path))
        yield ('longest', (self.longest_length, self.longest_path))

    def reducer(self, key, winners):
        """Reducer for Step 2. Takes the shortest and longest from several
        mappers and/or reducers and yields the overall winners in each category.

        The winners are a list of winners from several mappers OR reducers for
        the given key.

        Run this reducer enough and eventually you get to the final winner in
        each key/category.
        """
        if key == "shortest":
            yield (key, min(winners))
        if key == "longest":
            yield (key, max(winners))


if __name__ == '__main__':
    MRSalesman.run()
