author='Abdallah Sobehy, Fred Aklamanu'
author_email='abdallah.sobehy@telecom-sudparis.eu,fred.aklamanu@telecom-sudparis.eu',
date='1/12/2015'		

from pymongo import MongoClient				 # pymongo drivers and client instance
import networkx as nx
import matplotlib.pyplot as plt
import time

start_time = time.time()
client = MongoClient( 'localhost', 27017 )   # connect to the server and port 
db = client.dataset							 # connect to the dataset DB


G = nx.DiGraph() 							 # Directed to show the direction of the flow

collection_tuples = db.tuples
mytuples = []
tupleids = []

mytuples = list(collection_tuples.find(  { "src": { "$eq": "1316" } ,  "dst": { "$eq": "1983" } },  { "_id":0, "tuple":1   }  ))


for record in mytuples:
 	tupleids.append(record['tuple'])
 	


print 'Total records found for tupleids in tuple collection: ', len(mytuples)


collection_edgetuple = db.edgetuple
myedgetuples = []
edgeids = []
myedgetuples = list(collection_edgetuple.find( { "tuple": { "$in": tupleids } }, { "_id": 0, "edge": 1 }  ))
 
			

for record in myedgetuples:
 	edgeids.append(record['edge'])


print 'Total records found for edgeids in edgetuple collection: ', len(edgeids)



collection_edges = db.edges
myedges = []
myedges = list(collection_edges.find( { "edge": { "$in": edgeids } }, { "_id": 0, "ip1": 1, "ip2":2, "l12":3 }  ))



print 'Total records found for edgeids in edge collection: ', len(myedges)


for record in myedges:
     ip1_id = int(record['ip1'])
     ip2_id = int(record['ip2'])
     hop_diff = int(record['l12'])
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
print 'Time elapsed %f' % (time.time() - start_time)
