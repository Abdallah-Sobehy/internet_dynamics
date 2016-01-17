# Filters edge_tuple for resolved tuples with h2 - h1 = 2
import pymongo as pm
import time
import csv
import random
##
# @param edge_id ID of edge to extract info about
# return (ip1_id,ip2_id,hop_diff)
def get_ip12(edge_id):
    edge = c_edges.find_one({"edge":str(edge_id)})
    return (int(edge['ip1']),int(edge['ip2']),int(edge['l12']))
##
# Resolves a 1* if possible by removing its entry in edgetuple file and update the duration of the replacing edges
# @param ip1_r first ip of the edge to be resolved
# @param ip1_r second ip of the edge to be resolved
# @param ip1_p the first ip of the first edge to replace the edge to be resolved
# @param ip2_p the second ip of the first edge to replace the edge to be resolved
# @param ip1_n the first ip of the second edge tp replace the edge to be resolved
# @param ip2_n the second ip of the second edge tp replace the edge to be resolved
def resolve(ip1_r,ip2_r, ip1_p, ip2_p, ip1_n, ip2_n):
    if int(ip1_r) == int(ip1_p) and int(ip2_r) == int(ip2_n) and int(ip2_p) == int(ip1_n):
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
edges_checked = 0 # Number of edges checked for resolving
edges_resolved = 0 # Number of edges successfully resolved
start_time = time.time()
# maximum number of tuples to be checked to resolve an edge
max_tuples_check = 5
# Loop all the edges in the edges file where only edges with one star are stored
with open('edges_diff_2.csv', 'rb') as csvfile:
    next(csvfile) # To ignore the first line
    reader = csv.reader(csvfile, delimiter=';')
    for row in reader:
        e_resolve = row[0]
        ip1_r = row[1]
        ip2_r = row[2]
        
        print 'Edge to be resolved [ ', e_resolve,' ]', ip1_r, ' -> ' , ip2_r 
        # TODO get all edges not only one !!
        resolve_tuples = c_edgetuple.find({"edge": e_resolve})
        max_iter = random.randint(1, max_tuples_check)
        i = 0 
        for edgetuple_r in resolve_tuples:
            if i == max_iter:
                break
            else: i += 1
            # Find the tuple record corresponding to the edge to be resolved.
            t_r = c_tuples.find_one({"tuple": edgetuple_r['tuple']})
            src = t_r['src']
            dst = t_r['dst']
            h1_resolve = t_r['h1']
            h2_resolve = t_r['h2']
            # Find the tuple with the same source and the destination as t_r and with h1 as t_r but h2 = h1 + 1 (no star)
            mid_tuple = c_tuples.find_one( {"src": src, "dst": dst, "h1": h1_resolve , "h2": str(int(h1_resolve)+1)})
            if mid_tuple != None:
                print 'Mid tuple: '+ str(mid_tuple['tuple'])
                # Find corresponding edge for mid tuple
                mid_edge = c_edgetuple.find_one({"tuple": mid_tuple['tuple']})
                (ip1_m,ip2_m,h_diff_m) = get_ip12(mid_edge['edge'])
                print "Mid Edge [",mid_edge['edge'] ,"]:", ip1_m, "->", ip2_m
                # Condition to look forward got the '*' node by finding the tuple with h1 = h1_resolve +1 and h2 = h1_resolve + 1
                if int(ip1_r) == int(ip1_m):
                    next_tuple = c_tuples.find_one({"src": src,"dst": dst, "h1":str(int(h1_resolve)+1), "h2": str(int(h1_resolve)+2)})
                    if next_tuple != None:
                        print 'next tuple: '+ str(next_tuple['tuple'])
                        # Get the edge corresponding to the next tuple
                        next_edge = c_edgetuple.find_one({"tuple": next_tuple['tuple']})
                        (ip1_n,ip2_n,h_diff_n) = get_ip12(next_edge['edge'])
                        print "Next Edge: ", ip1_n, "->", ip2_n
                        # call resolve function to verify that mid and next edges are sufficient to replace the edge to be resolved
                        if(resolve(ip1_r,ip2_r, ip1_m, ip2_m, ip1_n, ip2_n)):
                            edges_resolved += 1
                            break
                # Condition to look backwards for the '*' node by finding the tuple with h1 = h1_resolve -1 and h2 = h1_resolve
                elif int(ip2_m) == int(ip2_r):
                    prev_tuple = c_tuples.find_one( {"src": src, "dst": dst, "h1": str(int(h1_resolve) - 1) , "h2": h1_resolve})
                    if prev_tuple != None:
                        print 'previous tuple: '+ str(prev_tuple['tuple'])
                         # Get the edge corresponding to the prev tuple
                        prev_edge = c_edgetuple.find_one({"tuple": prev_tuple['tuple']})
                        (ip1_p,ip2_p,h_diff_p) = get_ip12(prev_edge['edge'])
                        print "Prev Edge: ", ip1_p, "->", ip2_p
                        # call resolve function to verify that mid and prev edges are sufficient to replace the edge to be resolved
                        if(resolve(ip1_r,ip2_r,ip1_p,ip2_p, ip1_m, ip2_m)):
                            edges_resolved += 1
                            break
            
        edges_checked += 1
        print 'edges resolved: ', edges_resolved, '/', edges_checked
        print '----------------------------------------------'
            
print 'Time elapsed %f' % (time.time() - start_time)