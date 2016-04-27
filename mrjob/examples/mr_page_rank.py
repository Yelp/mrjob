# Copyright 2009-2010 Yelp
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
"""Iterative implementation of the PageRank algorithm:

http://en.wikipedia.org/wiki/PageRank
"""
from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol
from mrjob.step import MRStep


def encode_node(node_id, links=None, score=1):
    """Print out a node, in JSON format.

    :param node_id: unique ID for this node (any type is okay)
    :param links: a list of tuples of ``(node_id, weight)``; *node_id* is the
                  ID of a node to send score to, and *weight* is a number
                  between 0 and 1. Your weights should sum to 1 for each node,
                  but if they sum to less than 1, the algorithm will still
                  converge.
    :type score: float
    :param score: initial score for the node. Defaults to 1. Ideally, the
                  average weight of your nodes should be 1 (but it if isn't,
                  the algorithm will still converge).
    """
    node = {}
    if links is not None:
        node['links'] = sorted(links.items())

    node['score'] = score

    return JSONProtocol.write(node_id, node) + '\n'


class MRPageRank(MRJob):

    INPUT_PROTOCOL = JSONProtocol  # read the same format we write

    def configure_options(self):
        super(MRPageRank, self).configure_options()

        self.add_passthrough_option(
            '--iterations', dest='iterations', default=10, type='int',
            help='number of iterations to run')

        self.add_passthrough_option(
            '--damping-factor', dest='damping_factor', default=0.85,
            type='float',
            help='probability a web surfer will continue clicking on links')

    def send_score(self, node_id, node):
        """Mapper: send score from a single node to other nodes.

        Input: ``node_id, node``

        Output:
        ``node_id, ('node', node)`` OR
        ``node_id, ('score', score)``
        """
        yield node_id, ('node', node)

        for dest_id, weight in node.get('links') or []:
            yield dest_id, ('score', node['score'] * weight)

    def receive_score(self, node_id, typed_values):
        """Reducer: Combine scores sent from other nodes, and update this node
        (creating it if necessary).

        Store information about the node's previous score in *prev_score*.
        """
        node = {}
        total_score = 0

        for value_type, value in typed_values:
            if value_type == 'node':
                node = value
            else:
                assert value_type == 'score'
                total_score += value

        node['prev_score'] = node['score']

        d = self.options.damping_factor
        node['score'] = 1 - d + d * total_score

        yield node_id, node

    def steps(self):
        return ([MRStep(mapper=self.send_score, reducer=self.receive_score)] *
                self.options.iterations)


if __name__ == '__main__':
    MRPageRank.run()
