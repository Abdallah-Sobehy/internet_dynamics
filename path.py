import csv
import networkx as nx
import matplotlib.pyplot as plt
import time
start_time = time.time()

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
print 'Time elapsed %f' % (time.time() - start_time)