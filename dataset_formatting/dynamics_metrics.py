# Extracts the tuples where h2 - h1 > 1
import pymongo as pm
import time
import csv
import os

start_time = time.time()
# round_path output file

start_round = 1
stop_round = 2


f = open("dynamicMetrics20.csv", "w" )
f.write('start_round;stop_round;appearance;disappearance\n')
f.write(str(start_round) +';'+ str(stop_round)+';'+'[')
client = pm.MongoClient()
db = client.dataset
c_dynamics = db.dynamics
dynamic_metric = 0
dynamic_edge = {}
prev_rnd_edges = {}
current_rnd_edges = {}
next_rnd_edges = {}

prev_rnd_edges = c_dynamics.find_one({"r":1})
tmp = str(prev_rnd_edges['edges']).translate(None,'[] ').split(';')
for e in tmp:
	prev_rnd_edges[e] = 1

current_rnd_edges = c_dynamics.find_one({"r":2})
tmp = str(current_rnd_edges['edges']).translate(None,'[] ').split(';')
for e in tmp:
	current_rnd_edges[e] = 1

def write_appearance(current,prev):
	count = 0 
	app_occured = False
	for key, value in current.iteritems():
		if key not in prev:
			f.write(key+',')
			count +=1
			app_occured = True
	if app_occured:	
		f.seek(-1, os.SEEK_END)
		f.truncate()
	f.write('];[')
	print 'appearance count: ', count 

def write_disappearance(current,prev):
	count = 0
	disapp_occured = False
	for key, value in prev.iteritems():
		if key not in current:
			f.write(key+',')
			count += 1
			disapp_occured = True
	if disapp_occured:		
		f.seek(-1, os.SEEK_END)
		f.truncate()
	f.write(']\n')
	print 'disappearance count: ', count 


write_appearance(current_rnd_edges, prev_rnd_edges)
write_disappearance(current_rnd_edges, prev_rnd_edges)
		

for rnd in xrange( 2, 24 ):
	f.write(str(rnd)+';'+str(rnd+1)+';[')
	prev_rnd_edges = current_rnd_edges

	current_rnd_edges = c_dynamics.find_one({"r":(rnd+1)})
	tmp = str(current_rnd_edges['edges']).translate(None,'[] ').split(';')
	for e in tmp:
		current_rnd_edges[e] = 1
	write_appearance(current_rnd_edges, prev_rnd_edges)
	write_disappearance(current_rnd_edges, prev_rnd_edges)
	# dynamic_edge = list(c_edgetuple.find( "$and" { : [ {"r": { "$eq": "start_round" }, { "r"} ] } , { "_id":0, "edges":1 } )


print 'Time elapsed %f' % ( time.time() - start_time )
