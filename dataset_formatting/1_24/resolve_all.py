# Filters edge_tuple for resolved tuples with h2 - h1 > 2 with the dataset all in one collection.
import pymongo as pm
import time
import csv
import random
from numpy import genfromtxt
##
# Returns a list of tuples pointing to the input edge
# @param edge the edge id to extract tuples pointing too
# @param max_tuple the maximum number of different tuples to be returned
def get_tuples_for_edge(edge,max_tuples):
    tuples_list = []
    resolve_records = c_dataset.find({"edge": e_resolve})
    i = 0
    for record in resolve_records:
        if i == max_tuples:
            return tuples_list
        elif record['tuple'] not in tuples_list:
            tuples_list.append(record['tuple'])
            i += 1
    # In case the found tuples are less than max_tuple, just return the found
    return tuples_list

##
# Combines different possible replacement lists to resolve the maximum possible stars
# Replaces after resolving by writing into corresponding files 
# @param replacements list of lists of ips to replace the stars
# @return 
def combine_replace(edge, replacements, l12_r, ips_to_edge):
	if len(replacements) == 0 : return
	else:
		replacing_edges = []
		ip1 = replacements[0][0]
		for i in xrange(1, l12_r+1):
			print 'in index ', i 
			for l in replacements:
				print 'checking one list'
				if l[i] != '-1':
					# print 'hey roger',(ip1+l[i]) 
					if (ip1+l[i]) in ips_to_edge:
						print 'adding edge ', ips_to_edge[ip1+l[i]]
						replacing_edges.append(ips_to_edge[ip1+l[i]])
						ip1 = l[i]
						break
					else: 	
						record_tmp = c_dataset.find_one({"ip1": ip1, "ip2": l[i]})
						if record_tmp != None:
							replacing_edges.append(str(record_tmp['edge']))
							ip1 = str(record_tmp['edge'])
							break
		
		print 'final replacing_edges : ', replacing_edges



client = pm.MongoClient()
db = client.dataset
c_dataset = db.dataset_all_1_24

total_edges_checked = 50
start_line = 0 # line to start reading the edges from
edges_checked = 1 # Number of edges checked for resolving
edges_resolved = 0 # Number of edges successfully resolved
start_time = time.time()
Edges = genfromtxt('Edges_1_24.csv', delimiter=';', skip_header=0, skip_footer=0)
Tuples = genfromtxt('Tuples_1_24.csv', delimiter=';', skip_header=0, skip_footer=0)
print "Size of the Edges (rows, #attributes) ", Edges.shape
print "Size of the Tuples (rows, #attributes) ", Tuples.shape
# File to store reolved edges and their replacement
f = open('edges_resolved.csv', "a")
f.write("edge;replacements\n")

# Loop Edges to be resolved in ascending order of the number of stars
edge_l12 = 2
j = 0
while edge_l12 > 1:
    if j == total_edges_checked: break
    file_name = 'Edges_diff_'+str(edge_l12)+'.csv'
    print 'resolving edges with l12 = ', edge_l12 
    # Loop all the edges in the edges file where only edges with one star are stored
    with open('./Edges_1_24_diff/'+file_name, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        for row in reader:
            # To stop resolving when total_edges_checked is reached
            if j == total_edges_checked: break
            else: j+= 1
            e_resolve = row[0]
            ip1_r = row[1]
            ip2_r = row[2]
            l12_r = int(row[3])
            resolved = False
            print '========================'
            print 'Edge to be resolved [ ', e_resolve,' ]', ip1_r, ' -> ' , ip2_r 
            # Get tuples pointing at the same edge.
            tuples = get_tuples_for_edge(e_resolve,5)
            print 'number of found tuples = ', len(tuples)
            replacements_list = []
            # Loop tuples
            for t in tuples:
                print '\ntuple: ', t, 'h1 = ', str(Tuples[t,3])[:-2], 'h2 = ', str(Tuples[t,4])[:-2]
                if str(Tuples[t,4])[:-2] == 31:
                	print 'Will not examine tuple with h2 == 31.'
                	continue
                # List to store replacing edges
                replacing_edges = []
                replacing_ips = []
                ips_to_edge = {}
                # add the ip1 of the current edge because it has to be a part of the replacement
                replacing_ips.append(ip1_r)
                # The index of the last non star ip found
                known_ip_index = 0
                # Loop number of maximum possible replacing edges (if all stars resolved)= l12
                h1_index = str(Tuples[t,3])[:-2]
                h1_h2_diff = 1
                for j in xrange(l12_r):
                    print 'searched: h1 = ', h1_index, ' h2 = ', str(int(h1_index)+h1_h2_diff), ' ip1 must be = ' , replacing_ips[known_ip_index]
                    # find tuple with h1 = h1_r and h2 = h1_r + 1 [store its edge] if found the h1 for next round is +=1 and h2 should be reset to h1_r + 1 (because it might be incremented by th next line)
                    record_tmp = c_dataset.find_one({"src": str(Tuples[t,1])[:-2], "dst": str(Tuples[t,2])[:-2], "h1": h1_index , "h2": str(int(h1_index)+h1_h2_diff)})
                    # If not found h1 stays the same but the h2 is moved one step further from h2
                    if record_tmp != None:
                    	print 'edge to be tested for validity [ ', record_tmp['edge'] ,' ]', record_tmp['ip1'], ' -> ' , record_tmp['ip2'] 
                    if record_tmp == None or record_tmp['ip1'] != replacing_ips[known_ip_index]:
                        replacing_ips.append('-1')
                        h1_h2_diff +=1
                        continue
                    else: # Add the replacing edge to the list and move h1_index and reset h1_h2_diff
                        replacing_edges.append(record_tmp['edge'])
                        print 'replacing Edge [ ', record_tmp['edge'] ,' ]', record_tmp['ip1'], ' -> ' , record_tmp['ip2'] 
                        replacing_ips.append(str(record_tmp['ip2']))
                        # Update ip to edge dictionary
                        ips_to_edge[str(record_tmp['ip1'])+str(record_tmp['ip2'])] = str(record_tmp['edge'])
                        h1_index =  str(int(h1_index) + h1_h2_diff)
                        h1_h2_diff = 1 
                        known_ip_index = len(replacing_ips) - 1
                # Check if there is not one star then this tuple is enough to resolve the edge fully
                if '-1' not in replacing_ips:
                	print 'Edge fully resolved from one tuple : ', replacing_ips
                	# Reinitialize replacements list
                	replacements_list = []
                	# call combine_replace function with one list in replacements_list
                	replacements_list.append(replacing_ips)
                	resolved = True
                	print 'ips_to_edge', ips_to_edge
                	combine_replace(e_resolve, replacements_list, l12_r, ips_to_edge)
                	break
                else:
                	print 'possible replacing ips: ', replacing_ips
                	replacements_list.append(replacing_ips)
            if resolved == False:
				print 'Replacements from different tuples: ', replacements_list	
				combine_replace(e_resolve, replacements_list, l12_r, ips_to_edge)        
                # Check the ip2 of the last edge in the replacements list must be = to ip2_r otherwise not accepted
                # If true the list is saved in replacements_list with unknown edges saved as -1 
                # A condition to avoid unioning lists is if there is no unknown edges == all stars resolved so no need for making union
            # If none of the lists has all stars resolved we call a function to union them
                # 2 nested loops
                    # in a list if an unknown edges is encountered, the same index is searched in all other lists
                    # If a known edge is found in that index the ips are checked to see if they have one in common
                    # If one can replace this unknown edge the looping continues from the list with the found edge
            # Finally the resolved 
    
    # Update the edge_l12 with the next value to be resolved
    # Ignore edge with stars 17,18,19,20 as they do not exist
    if edge_l12 == 21:
        edge_l12 = 16
    else: edge_l12 -= 1
    print '---------------------------------------------------'
    

print 'Time elapsed %f' % (time.time() - start_time)
    
