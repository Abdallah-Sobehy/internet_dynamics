# authors: Abdallah Sobehy and Fred Aklamanu
# description: This script determines the total number of edgesIDs found per round(1..24). 
# All edgeIDs are placed in an array for the round.

import pymongo as pm
import time
import csv

start_time = time.time()
# round_path output file

start_round = 1
stop_round = 24

# Creates a file and with header, round and edges
f = open("EdgeDynamics.csv", "w")
f.write('r,edges\n')

#External connection to MongoDB
client = pm.MongoClient()
db = client.dataset
c_edgetuple = db.edgetupleoriginal
c_edges = db.edges
dynamic_metric = 0
dynamic_edge = {}



for rnd in xrange(start_round, stop_round+1):
    f.write(str(rnd)+',[1')
    dynamic_edge['1'] = 1
    with open('EdgeTuple.csv', 'rb') as csvfile:
        next(csvfile) # To ignore the first line
        reader = csv.reader(csvfile, delimiter=',')
        for record in reader:
            if ( int(record[2]) <= rnd and int(record[3]) >= rnd ):
                if record[1] not in dynamic_edge:
                    dynamic_edge[record[1]] = 1
                    f.write(';' + record[1] )
                
    f.write('];' + str(len(dynamic_edge)) + '\n')
    print 'Round finished'


    print 'Dictionary length: ', len(dynamic_edge)
    
    print 'Time elapsed %f' % ( time.time() - start_time )