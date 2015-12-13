# Creates a file: round;source;destination;path from Tupls, EdgeTuple, Edges file
import pymongo as pm
import time
import csv
##
# gets the Id of the edge corresponding to the input tuple ID and was present in round r
# @param tuple_id tuple ID 
# @param r round where the edge existed
# return the edge ID if an edge existed for the input tuple and round
# Otherwise returns -1
def edge_in_round(tuple_id, r):
    edgetuple = c_edgetuple.find( {"tuple": str(tuple_id)})
    for e_t in edgetuple:
        # Convert the start/stop string in list format to a list
        start_arr = str(e_t['start']).translate(None,'[] ').split(',')
        stop_arr  = str(e_t['stop']).translate(None,'[] ').split(',')
        # Loop the start/stop arrays to make sure the edge is found in the input round
        for i in xrange(len(start_arr)):
            if int(start_arr[i]) <= r and int(stop_arr[i]) >= r:
                print 'good edge presence: ', start_arr[i], ' - ', stop_arr[i] 
                return int(e_t['edge'])
    print 'Edge not found for tuple ID: ' , tuple_id
    return -1
##
# @param edge_id ID of edge to extract info about
# return (ip1_id,ip2_id,hop_diff)
def get_ip12(edge_id):
    edges = c_edges.find({"edge":str(edge_id)})
    for e in edges:
        print e['ip1'],e['ip2'],e['l12']
        return (int(e['ip1']),int(e['ip2']),int(e['l12']))
    
start_time = time.time()
# round_path output file
f = open("round_path_2.csv", "w")
f.write('r;src;dst;path\n')

# The first round to include its path in the file
start_round = 1
# The last round to include its path in the file
stop_round = 23

client = pm.MongoClient()
db = client.dataset
c_tuples = db.tuples 
c_edgetuple = db.edgetuple
c_edges = db.edges 

# Get all tuples for a src and dst
tuples = list(c_tuples.find( {"src": "1316", "dst": "1983"}))

# Create a dictionary {tuple_id:[h1,h2]}
tuples_hops = dict((int(t['tuple']),[int(t['h1']),int(t['h2'])]) for t in tuples)

# for k,v in tuples_hops.iteritems():
#     print k, ':', v[0], ':', v[1]
print 'number of tuples: ', len(tuples_hops)

# loop for all rounds to write its path in the file
for r in xrange(start_round,stop_round+1):
    f.write(str(r)+';'+'1316'+';'+'1983'+';')
    # a dictionary to map h1 to [h2,edge]  for each round
    hop_edge = {}
    for t,h in tuples_hops.iteritems():
        print 'tuple input: ' t
        # Get the edge ID of that tuple that was present in round r
        edge = edge_in_round(str(t),r)
        if edge != -1:
            print 'updating hop_edge with edge: ', edge
            # Uses the tuples_hops to get h1 as key and maps it to h2 and the found edge
            hop_edge[tuples_hops[t][0]] = (tuples_hops[t][1],edge)
    i = 0
    round_edges = []
    while i < len(hop_edge):
        if i in hop_edge:
            print 'calling get_ip12'
            (ip1,ip2,h_diff) = get_ip12(hop_edge[i][1])
            round_edges.append(ip1)

            if  h_diff > 1 and h_diff != 31:
                for j in xrange(h_diff - 1):
                    round_edges.append(-1)
            elif h_diff == 31:
                round_edges.append(-31)
        i += 1

    f.write('[')
    for e in round_edges:
        f.write(str(e) + ',')
    f.write('1983]\n')
    print 'round finished: ', r
    print '---------------------------------------'

print 'Time elapsed %f' % (time.time() - start_time)