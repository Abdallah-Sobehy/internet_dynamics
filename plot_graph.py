## @package plot_graph
# Contains functions for plotting graphs (mainly from mongodb files)
# authors='Abdallah Sobehy, Fred Aklamanu'
# authors_email='abdallah.sobehy@telecom-sudparis.eu,fred.aklamanu@telecom-sudparis.eu',

import csv
import networkx as nx
import matplotlib.pyplot as plt
import time

##
# Draws the graph of the internet between the input rounds inclusive
# If start == stop means the graph for 1 round
# @param start the first included round
# @param stop the last included round
#
def internet_graph(start_in, stop_in):
	# In EdgeTuple 
	edge_ids = []
	client = MongoClient( 'localhost', 27017 )
	db = client.dataset
	G = nx.DiGraph()
	# edgeTuple collection in the non array form for start and stop
	edgeTuple_c = db.edgetupleoriginal
	# get edgetuple records where start_in <=start<=stop_in OR start_in <=stop<=stop_in OR start < start_in and stop > stop_in
	edges_dict = list(edgeTuple_c.find( { "$or": [ { "$and": [ {"start": { "$gte": str(start_in) } }, {"start": { "$lte": str(stop_in) } } ] }, { "$and": [ {"stop": { "$gte": str(start_in) } }, {"stop": { "$lte": str(stop_in) } } ] }, { "$and": [ {"start": { "$lt": str(start_in) } }, {"stop": { "$gt": str(stop_in) } } ] } ] }, { "_id":0, "edge":1 } ))
	#f=open("tupleid.txt", "w")
	#store edges
	for record in edges_dict:
		edge_ids.append(record['edge'])

	#print 'Size: ', len(edges_dict)
	# Write the edges into a file for testing puposes
	edgeids=[]
	# for record in edges_dict:
	# 	if record['edge'] not in edgeids:
	# 		f.write(record['edge'] + "\n")
	# 		edgeids.append(record['edge'])
	# print 'unique edges: ' , len(edgeids)

	# Get edges records in edgeids 
	collection_edges = db.edges
	myedges = list(collection_edges.find( { "edge": { "$in": edgeids } }, { "_id": 0, "ip1": 1, "ip2":2, "l12":3 }  ))

	for record in myedges:
	    ip1_id = int(record['ip1'])
	    ip2_id = int(record['ip2'])
	    hop_diff = int(record['l12'])
	    # Simulate an unknown nodes '*' as a node with a label = the hop difference
	    if hop_diff > 1:
	        intermediate = str(ip1_id)+str(ip2_id)
	        int_label = str(hop_diff)+'*'
	        G.add_node(intermediate, label =int_label)
	        G.add_edge(ip1_id,intermediate)
	        G.add_edge(intermediate,ip2_id)
	    else:
	        G.add_edge(ip1_id,ip2_id)

	nx.write_gexf(G, "whole_graph.gexf")

##
# Draws all paths found between the input source and destination for all rounds reading from mongodb collections.
# @param source ip id
# @param destination ip id 
# Writes the graph in a .gexf file
def draw_path_mongodb(source, destination):
	
	# connect to the server and port 
	client = MongoClient( 'localhost', 27017 )   
	# connect to the dataset DB
	db = client.dataset
	# Directed to show the direction of the flow
	G = nx.DiGraph()

	collection_tuples = db.tuples
	mytuples = []
	tupleids = []
	# get all tuples where source and destination
	mytuples = list(collection_tuples.find(  { "src": { "$eq": "$source" } ,  "dst": { "$eq": "$destination" } },  { "_id":0, "tuple":1   }  ))
	# Store tuple ids in a list
	for record in mytuples:
	 	tupleids.append(record['tuple'])
	print 'Total records found for tupleids in tuple collection: ', len(mytuples)

	collection_edgetuple = db.edgetuple
	myedgetuples = []
	edgeids = []
	# Get all edge tuples records in tupleids
	myedgetuples = list(collection_edgetuple.find( { "tuple": { "$in": tupleids } }, { "_id": 0, "edge": 1 }  ))
	# Srote edges to be drawin in a list
	for record in myedgetuples:
	 	edgeids.append(record['edge'])
	print 'Total records found for edgeids in edgetuple collection: ', len(edgeids)

	collection_edges = db.edges
	myedges = []
	# Get all edge records in edgeids
	myedges = list(collection_edges.find( { "edge": { "$in": edgeids } }, { "_id": 0, "ip1": 1, "ip2":2, "l12":3 }  ))
	print 'Total records found for edgeids in edge collection: ', len(myedges)
	# Get node info from the myedges list
	for record in myedges:
	     ip1_id = int(record['ip1'])
	     ip2_id = int(record['ip2'])
	     hop_diff = int(record['l12'])
	     # Simulate an unknown nodes '*' as a node with a label = the hop difference
	     if hop_diff > 1:
	         intermediate = str(ip1_id)+str(ip2_id)
	         int_label = str(hop_diff)+'*'
	         G.add_node(intermediate, label =int_label)
	         G.add_edge(ip1_id,intermediate)
	         G.add_edge(intermediate,ip2_id)
	     else:
	         G.add_edge(ip1_id,ip2_id)

	nx.draw_networkx(G, with_labels=True, node_size = 500)
	plt.axis('off')
	plt.show()
	nx.write_gexf(G, "pathmongo.gexf")

##
# Draws all paths found between the input source and destination for all rounds from csv files.
# @param source ip id
# @param destination ip id 
# Writes the graph in a .gexf file
def draw_path(source,destination):
	

	G = nx.DiGraph() # Directed to show the direction of the flow
	# Source and destination nodes of the path
	src = 1316
	dst = 1983
	# Dictionary that maps a tuple to its h1 and h2
	tuples_hops = {}
	# A list to store all edges
	edges = []
	# A dictionary that maps an edge to its start and stop times
	# {edge_id: [start1,stop1,start2,stop2...]}
	edge_presence = {} 
	edge_hops = {}
	# Storing tuple ID's for the source and destination
	with open('PartialDataset/Tuples.csv', 'rb') as csvfile:
	    next(csvfile) # To ignore the first line
	    reader = csv.reader(csvfile, delimiter=',')
	    for row in reader:
	        tuple_id = int(row[0])
	        h1 = int(row[3])
	        h2 = int(row[4])
	        if (int(row[1]) ==  src and int(row[2]) == dst ):
	            tuples_hops[tuple_id] = [h1,h2]
	            if (h1 == 1):
	                print tuples_hops[tuple_id]
	    print 'number of Tuples: ' + str(len(tuples_hops))

	# Fetching the corresponding edge ID in EdgeTuples.csv
	with open('PartialDataset/EdgeTuple.csv', 'rb') as csvfile:
	    next(csvfile) # To ignore the first line
	    reader = csv.reader(csvfile, delimiter=',')
	    for row in reader:
	        tuple_id = int(row[0])
	        edge_id = int(row[1])
	        start = int(row[2])
	        stop = int(row[3])
	        # if the edge belong the tuples of the src and dst
	        if tuples_hops.has_key(tuple_id):
	            #store edges of the tuples in the edge list if they were not already there
	            if edge_id not in edges:
	                edges.append(edge_id)
	                # Create the key, value {edge_id: [start,stop]} of edge_presence dictionary
	                edge_presence[edge_id] = [start,stop]
	                edge_hops[edge_id] = tuples_hops[tuple_id]
	            # Else if the edge already existed, only update the dictionary with the new presence duration
	            else:
	                presence = edge_presence[edge_id]
	                presence.append(start)
	                presence.append(stop)
	                # Update dictionary
	                edge_presence[edge_id] = presence
	    print 'number of edges: ' + str(len(edges))
	    for edge in edges:
	    	print edge

	# Fetch the edges that belong to the path in EDges.csv file and draw nodes at their edges
	with open('PartialDataset/Edges.csv', 'rb') as csvfile:
	    next(csvfile) # To ignore the first line
	    reader = csv.reader(csvfile, delimiter=',')
	    for row in reader:
	        edge_id = int(row[0])
	        ip1_id = int(row[1])
	        ip2_id = int(row[2])
	        hop_diff = int(row[3])
	        if int(row[0]) in edges:
	            # If it is the first hop from the source, add the source node and attach it to ip1_id
	            # if edge_hops[edge_id][0] == 1:
	            #     G.add_edge(src,ip1_id)

	            # if the hop difference is more than 1, insert a node to express the * count
	            if hop_diff > 1:
	                intermediate = str(ip1_id)+str(ip2_id)
	                int_label = str(hop_diff)+'*'
	                G.add_node(intermediate, label =int_label)
	                G.add_edge(ip1_id,intermediate)
	                G.add_edge(intermediate,ip2_id)
	            else:
	                G.add_edge(ip1_id,ip2_id)

	if (G.has_node(src)):
	    print 'Graph has source'

	if (G.has_node(dst)):
	    print 'Graph has destination'

	nx.draw_networkx(G, with_labels=True, node_size = 500)
	plt.axis('off')
	plt.show()
	nx.write_gexf(G, "path.gexf")

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
	    			if ! ((int(start_arr[i]) < start_in and int(stop_arr[i]) < start_in) or (int(start_arr[i]) > stop_in and int(stop_arr[i]) > stop_in)):
	    				good_edge = True
	    		if good_edge == False: raise SystemExit('Edge: ', edge_id, ' is not in the good range')
	    		else: good_edge = False 
	    return True