# The aim of this script is to maniplulate the dataset to remove EdgeTuple records where start == stop
# First the original data set has so be separated with ; (to be able to have arrays) with the following lines
# sed -e 's/,/;/g' EdgeTuple_test.csv > dst_file

import csv
import networkx as nx
import matplotlib.pyplot as plt
import time
start_time = time.time()
valid_f = open('PartialDataset/validEdgeTuple.csv','w')
invalid_f = open('PartialDataset/invalidEdgeTuple.csv','w')
# Traverse the orignal EdgeTuple file and store start,stop arrays in dictonary form
with open('PartialDataset/EdgeTuple.csv', 'rb') as csvfile:
    header = next(csvfile) # To ignore the first line
    valid_f.write(header)
    invalid_f.write(header)
    reader = csv.reader(csvfile, delimiter=';')
    for row in reader:
        row_str = row[0]+ ';'+ row[1]+';'+ row[2]+';'+ row[3]
        #print row_str
        tuple_id = int(row[0])
        edge_id = int(row[1])
        start = int(row[2])
        stop = int(row[3])
    	# If The record is valid write it in validEdgeTuples.csv
    	if start != stop:
    		valid_f.write(str(row_str)+'\n')
    	else: invalid_f.write(str(row_str)+'\n')