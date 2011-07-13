mr_travelling_salesman
=====================

mr_travelling_salesman is an example of an mrjob (github.com/Yelp/mrjob) for 
solving the travelling salesman problem. The idea is to demonstrate how to solve
a problem that doesn't involve a large data set but still requires a large 
amount of computing power.

How to Run
----------

::

    # locally
    python mr_travelling_salesman.py example_graphs/10_nodes.py
    # on Amazon Elastic Map/Reduce
    python mr_travelling_salesman.py example_graphs/10_nodes.py -v emr

Input Format
------------

The input consists of a JSON dictionary of two entries, 'graph' and 
'start_node', all on one line. 'graph' should be a list of list representing an
NxN matrix where each entry in represents the cost of travelling from the row
index to the column index on the graph. 'start_node' is an integer value
for the starting/ending node of the sales tour. See the examples in the
*example_graphs* directory.
