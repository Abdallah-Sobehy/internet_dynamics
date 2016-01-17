# Filters edge_tuple for resolved tuples with h2 - h1 = 2
import pymongo as pm
import time
import csv
##
# @param edge_id ID of edge to extract info about
# return (ip1_id,ip2_id,hop_diff)
def get_ip12(edge_id):
    edge = c_edges.find_one({"edge":str(edge_id)})
    return (int(edge['ip1']),int(edge['ip2']),int(edge['l12']))

def resolve(ip1_r,ip2_r, ip1_p, ip2_n):
    if ip1_r == ip1_p and ip2_r == ip2_n:
        print "Edge", ip1_r, "->", ip2_r, "can be resolved."
        return True
    else:
        print "Edge", ip1_r, "->", ip2_r, "Can not be resolved."
        return False

client = pm.MongoClient()
db = client.dataset
c_tuples = db.tuples 
c_edgetuple = db.edgetuple
c_edges = db.edges 
tuples_checked = 0
tuples_resolved = 0
start_time = time.time()
with open('tuples_diff_2.csv', 'rb') as csvfile:
    next(csvfile) # To ignore the first line
    reader = csv.reader(csvfile, delimiter=';')
    for row in reader:
        t_resolve = row[0]
        src = row[1]
        dst = row[2]
        h1_resolve = row[3]
        h2_resolve = row[4]
        print 'tuple to be resolved: ', t_resolve 
        # TODO get all edges not only one !!
        resolve_edges = c_edgetuple.find_one({"tuple": t_resolve}) 
        (ip1_r,ip2_r,h_diff_r) = get_ip12(resolve_edges['edge'])
        print "Edge to be resolved: ", ip1_r, "->", ip2_r

        mid_tuple = c_tuples.find_one( {"src": src, "dst": dst, "h1": h1_resolve , "h2": str(int(h1_resolve)+1)})
        if mid_tuple != None:
            print 'Mid tuple: '+ str(mid_tuple['tuple'])
            mid_edge = c_edgetuple.find_one({"tuple": mid_tuple['tuple']})
            if mid_edge != None:
                (ip1_m,ip2_m,h_diff_m) = get_ip12(mid_edge['edge'])
                print "Mid Edge: ", ip1_m, "->", ip2_m
                if ip1_r == ip1_m:
                    next_tuple = c_tuples.find_one({"src": src,"dst": dst, "h1":str(int(h1_resolve)+1), "h2": str(int(h1_resolve)+2)})
                    if next_tuple != None:
                        print 'next tuple: '+ str(next_tuple['tuple'])
                        next_edge = c_edgetuple.find_one({"tuple": next_tuple['tuple']})
                        (ip1_n,ip2_n,h_diff_n) = get_ip12(next_edge['edge'])
                        print "Next Edge: ", ip1_n, "->", ip2_n
                        if(resolve(ip1_r,ip2_r, ip1_m, ip2_n)):
                            tuples_resolved += 1
                elif ip2_m == ip2_r:
                    prev_tuple = c_tuples.find_one( {"src": src, "dst": dst, "h1": str(int(h1_resolve) - 1) , "h2": h1_resolve})
                    if prev_tuple != None:
                        print 'previous tuple: '+ str(prev_tuple['tuple'])
                        prev_edge = c_edgetuple.find_one({"tuple": prev_tuple['tuple']})
                        (ip1_p,ip2_p,h_diff_p) = get_ip12(prev_edge['edge'])
                        print "Prev Edge: ", ip1_p, "->", ip2_p
                        if(resolve(ip1_r,ip2_r,ip1_p,ip2_m)):
                            tuples_resolved += 1
        
        tuples_checked += 1
        print 'tuples resolved: ', tuples_resolved, '/', tuples_checked
        print '----------------------------------------------'
        
print 'Time elapsed %f' % (time.time() - start_time)