# Editing the EdgeTuple.csv file to contain start/stop columns as arrays
import csv
import networkx as nx
import matplotlib.pyplot as plt
import time
start_time = time.time()
# maps 'tuple_id,edge_id' to '[start1, start2...]', '[stop1,stop2...]'
start_dict = {}
stop_dict = {}
# Traverse the orignal EdgeTuple file and store start,stop arrays in dictonary form
with open('PartialDataset/EdgeTuple.csv', 'rb') as csvfile:
    next(csvfile) # To ignore the first line
    reader = csv.reader(csvfile, delimiter=';')
    for row in reader:
    	tuple_id = int(row[0])
        edge_id = int(row[1])
        start = int(row[2])
        stop = int(row[3])
        tuple_edge = str(tuple_id) + ';' + str(edge_id)
        # If the tuple edge pair already exists add start/stop to the array
        if start_dict.has_key(tuple_edge):
        	start_arr = start_dict[tuple_edge]
        	start_arr.append(start)
        	start_dict[tuple_edge] = start_arr

        	stop_arr = stop_dict[tuple_edge]
        	stop_arr.append(stop)
        	stop_dict[tuple_edge] = stop_arr
        else:
        	start_dict[tuple_edge] = [start]
        	stop_dict[tuple_edge] = [stop]


# Loop the dictionaries to write the EdgeTuple file in the new format

newEdgeTuple = open('PartialDataset/custom/EdgeTuple.csv', 'w')
for key, value in start_dict.iteritems():
	line = key + ';' + str(value) + ';' + str(stop_dict[key]) + '\n'
	newEdgeTuple.write(line)
print 'Time elapsed %f' % (time.time() - start_time)