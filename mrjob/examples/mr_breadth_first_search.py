
"""Testing breadth first search using variable length steps"""

from mrjob.job import MRJob
import re
import sys

WORD_RE = re.compile(r"[\w']+")


class BreadthFirstSearchJob(MRJob):

    #state index
    
    #0 = unvisited
    #1 = visited but not expanded
    #2 = expanded
    
    #purpose of this test is to show that the code will terminate on a total of 4 steps, because nodes in graph.rst have a max distance of 2 from the starting node ( node 0), 
    #and hence the breadth first search process can be completed before the 11 steps allocated provisionally

    def steps(self):
        
        return [self.mr(mapper=self.mapper)]+[self.mr(mapper=self.mapper_search,reducer=self.reducer_search)]*10
    
    def mapper(self, _, line):
        node_id, edges = line.strip().split('\t')
        edges = edges.split(',')
        if node_id=='1':
            distance = 0
            state = 1
        else:
            distance = sys.maxint
            state = 0
        yield node_id, (edges,distance,state)

    def mapper_search(self,node_id,value):
        self.increment_counter('termination','initiate')
        connected_nodes, distance_from_anchor_a, state = value
        #print node_id, connected_nodes, distance_from_anchor_a, state
        if state==1:
            yield node_id, (connected_nodes,distance_from_anchor_a,2)
            for target_id in connected_nodes:
                yield target_id, ([],distance_from_anchor_a+1,1)
        else:
            yield node_id, (connected_nodes,distance_from_anchor_a,state)
    
    def reducer_search(self,key,values):

        distance=sys.maxint
        state = 0
        connections = []
        for sub_connections, sub_distance, sub_state in values:
        
            connections +=sub_connections
            distance = min(sub_distance,distance)
            state = max(state,sub_state)
        if state==1:
            self.increment_counter('termination','necessary_counts')
        yield key, (connections, distance,state)

if __name__ == '__main__':
    BreadthFirstSearchJob.run()