## @package analysis
# Contains functions for computing stats, testing validity of other functions.
# authors='Abdallah Sobehy, Fred Aklamanu'
# authors_email='abdallah.sobehy@telecom-sudparis.eu,fred.aklamanu@telecom-sudparis.eu',
import csv
import networkx as nx
import time
##
# Testing the edges used to draw a graph with internet_graph function are the good ones
# The edges should be stored in a file called edgeid.txt (done when drawing the graph)
# @param start_in the start round used in the internet_graph function
# @param stop_in the stop round used in the internet_graph function
def test_internet_rounds(start_in, stop_in):
	# Read edges used in drawing the graph
	with open('PartialDataset/edgeids.txt', 'rb') as csvfile:
	    next(csvfile) # To ignore the first line
	    reader = csv.reader(csvfile, delimiter=';')
	    edges = []
	    for row in reader:
	    	edges.append(row[0])

	with open('PartialDataset/custom/EdgeTuple.csv', 'rb') as csvfile:
	    next(csvfile) # To ignore the first line
	    reader = csv.reader(csvfile, delimiter=';')
	    for row in reader:
	    	edge_id = int(row[1])
	    	if edge_id in edges:
	    		# Extracting values from the start list
	    		start_arr = row[2].translate(None,'[] ')
	    		start_arr = start_arr.split(',')
	    		stop_arr = row[3].translate(None,'[] ')
	    		stop_arr = stop_arr.split(',')
	    		# loop the indices of both lists to check if the edge is a good one
	    		# Assuming the edge is not good, the flag will be set to True if start_in or stop_in lie in the good range
	    		good_edge = False
	    		for i in xrange(len(start_arr)):
	    			if not ((int(start_arr[i]) < start_in and int(stop_arr[i]) < start_in) or (int(start_arr[i]) > stop_in and int(stop_arr[i]) > stop_in)):
	    				good_edge = True
	    		if good_edge == False: raise SystemExit('Edge: ', edge_id, ' is not in the good range')
	    		else: good_edge = False 
	    return True

##
# Counts the number of unknown routers [*] in Edges database
def unknown_nodes_in_edges():
	# number of unknown routers (between 2 nodes)
	stars_count = 0
	# number of records where hop_diff > 1 and not = 31
	records_count = 0
	# Fetch the edges
	with open('../PartialDataset/Edges.csv', 'rb') as csvfile:
	    next(csvfile) # To ignore the first line
	    reader = csv.reader(csvfile, delimiter=';')
	    for row in reader:
	        edge_id = int(row[0])
	        ip1_id = int(row[1])
	        ip2_id = int(row[2])
	        hop_diff = int(row[3])
	        if hop_diff > 1 and hop_diff != 31:
	            stars_count += hop_diff - 1
	            records_count += 1
	print "number of stars ", stars_count
	print "number of records ", records_count