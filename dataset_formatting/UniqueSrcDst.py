# Creates a file: src;dst
import pymongo as pm
import time
import csv
start_time = time.time()

# Create connection to MongoDB and collections
client = pm.MongoClient()
db = client.dataset
c_tuples = db.tuples 
c_edgetuple = db.edgetuple
c_edges = db.edges 
new_tuples = {}

# Create a file to store the unique source and destination found in the tuple file
f = open("uniqueSrcDst.csv","w")

# Get all tuples for a src and dst
tuples = c_tuples.find({ "src": { "$gt": "0"}, "dst": { "$gt": "0" } }, { "_id":"1", "src":"1", "dst":"1"} )

# Store only unique source and destination in the uniqueSrcDst.csv file
for index in tuples:
	tmp = str(index['src']) + str(index['dst'])
	if tmp not in new_tuples:
		new_tuples[tmp] = 1
		f.write( index['src']+';'+index['dst']+'\n')
	# if index['_id'] not in new_tuples or new_tuples[index['src']] != index['dst']:
	# 	new_tuples[index['_id']] = index['dst']
	# 	f.write( index['src']+';'+index['dst']+'\n')
	



print 'Time elapsed %f' % (time.time() - start_time)