# Extracts the tuples where h2 - h1 > 1
import pymongo as pm
import time
import csv

start_time = time.time()
# round_path output file
f = open("tuples_diff.csv", "w")
f.write('tuple;src;dst;h1;h2;diff\n')
client = pm.MongoClient()
db = client.dataset
c_tuples = db.tuples

tuples = list(c_tuples.find())

for record in tuples:
	diff = int(record['h2']) - int(record['h1']) 
	if diff > 1 and int(record['h2']) != 31:
		f.write(record['tuple']+';'+record['src']+';'+record['dst']+';'+record['h1']+';'+record['h2']+';'+str(diff)+'\n')
print 'Time elapsed %f' % (time.time() - start_time)