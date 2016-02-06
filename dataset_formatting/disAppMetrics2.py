# authors: Abdallah Sobehy and Fred Aklamanu
# description: This scripts tracks count of appeared and disapppeared edges found in the edgeDynamics.csv per round

import pymongo as pm
import time
import csv

start_time = time.time()
# round_path output file

start_round = 1
stop_round = 24
appCount  = 0
disappCount = 0


f = open("DisAppMetrics20.csv", "w")
f.write('start_round,stop_round;appCount;disappCount\n')
client = pm.MongoClient()
db = client.dataset
c_edges = db.edges
dynamic_edge = {}
counter  = 0
rnd = 1


    
with open('dynamicMetrics2.csv', 'rb') as csvfile:
    next(csvfile) # To ignore the first line
    reader = csv.reader(csvfile, delimiter=';')
    for record in reader:
        app = str(record[2]).translate(None,'[] ').split(',')
        disapp = str(record[3]).translate(None,'[] ').split(',')
        for edge in app:
            found_edge = c_edges.find_one( {"edge":edge}, {"l12":1})
            if int(found_edge['l12']) != 31:
                appCount += int(found_edge['l12'])
        print 'Appearance Count: %d' % appCount
        for edge in disapp:
            found_edge = c_edges.find_one( {"edge":edge}, {"l12":1})
            if int(found_edge['l12']) != 31:
                disappCount += int(found_edge['l12'])
        print 'Disappearance Count: %d' % disappCount
        
        end_rnd = rnd + 1
        f.write(str(rnd) + ';' + str(end_rnd) + ';' + str(appCount) + ';' + str(disappCount) + '\n')
        appCount = disappCount = 0
        print 'Round finished between ', rnd, end_rnd     
        rnd += 1
        print 'Time elapsed %f' % ( time.time() - start_time )
              
        

print 'Time elapsed %f' % ( time.time() - start_time )