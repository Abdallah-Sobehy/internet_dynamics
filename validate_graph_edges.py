# This script validates that the edges used for drawing the internet graph between 2 given rounds are the correct ones.

import csv
import time
start_time = time.time()

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
	    			if ! ((start_arr[i] < start_in and stop_arr[i] < start_in) or (start_arr[i] > stop_in and stop_arr[i] > stop_in)):
	    				good_edge = True
	    		if good_edge = False: raise SystemExit('Edge: ', edge_id, ' is not in the good range')
	    return True

if __name__ == "__main__":
	print test_internet_rounds(220,240)