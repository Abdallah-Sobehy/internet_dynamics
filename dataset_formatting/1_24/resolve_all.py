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
		# List of the final ips to replace stars
		final_ips = []
		replacing_edges = []
		ip1 = replacements[0][0]
		final_ips.append(ip1)
		# Loop all ips after the first one (already saved in ip1)
		for i in xrange(1, l12_r+1):
			for l in replacements:
				# If a non star ip then add corresponding edge
				if l[i] != '-1':
					if (ip1+l[i]) in ips_to_edge:
						replacing_edges.append(ips_to_edge[ip1+l[i]])
						final_ips.append(l[i])
						ip1 = l[i]
						break
					else: # if not found in the dictionary, mongodb will be called for cross checking	
						print '*******Cross checking Ip\'s ', ip1, '->' , l[i] 
						record_tmp = c_dataset.find_one({"ip1": ip1, "ip2": l[i]})
						if record_tmp != None:
							print 'Cross checking succeeded'
							replacing_edges.append(str(record_tmp['edge']))
							ips_to_edge[ip1 + str(record_tmp['ip2'])] = str(record_tmp['edge'])
							final_ips.append(str(record_tmp['ip2']))
							ip1 = str(record_tmp['ip2'])
							break
						
		print 'final replacing_ips : ', final_ips
		print 'final replacing_edges : ', replacing_edges
		# Call future_replace function only when more than one list was used in resolving process
		if len(replacements) > 1:
			future_replace(final_ips,replacements, ips_to_edge)
## 
# Returns the number of stars in a list of ips
# @param replacing_ips list of ips 
def get_star_num(replacing_ips):
	star_count = 0
	for i in xrange(1,len(replacing_ips)):
		if replacing_ips[i] == '-1':
			star_count += 1
	return star_count

##
# Uses the final replacing ips which is formed by combining replacing ips to resolve one star edges in one of the replacing lists
# @param final list with final ips afer combining different lists
# @param replacements_list a list that was used to compute the final ips
def future_replace(final, replacements_list, ips_to_edge):
	for sub in replacements_list:
		# First and last indces are not considered as they never contain a star
		for i in xrange(1, len(final) - 1):
			# Replace if the subreplacement has one star edfe and the final has a known ip, and the ips before and after the star is the same for both
			if sub[i] == '-1' and sub[i-1] != '-1' and sub[i+1] != '-1' and final[i] != '-1' and final[i-1] != '-1' and final[i+1] != '-1' and sub[i-1] == final[i-1] and sub[i+1] == final[i+1]:
				print 'sub replacing ips: ', sub
				print 'future replace of [ ', ips_to_edge[sub[i-1]+sub[i+1]],' ]', sub[i-1], ' -> ' , sub[i+1]
				print 'By: [ ', ips_to_edge[final[i-1]+final[i]],' ]', final[i-1], ' -> ' , final[i]
				print 'And: [ ', ips_to_edge[final[i]+final[i+1]],' ]', final[i], ' -> ' , final[i+1]



client = pm.MongoClient()
db = client.dataset
c_dataset = db.dataset_all_1_24

total_edges_checked = 2 # Number of edges to be checked checked for resolving
edges_checked = 0 # Edges checked for resolving so far
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
edge_l12 = 3
k = 0
while edge_l12 > 1:
	# Condition to check only specified number of edges
    if k == total_edges_checked: break
    file_name = 'Edges_diff_'+str(edge_l12)+'.csv'
    print '//////////////////////////\nresolving edges with l12 = ', edge_l12 
    # Loop all the edges in the edges file where only edges with one star are stored
    with open('./Edges_1_24_diff/'+file_name, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        for row in reader:
            # To stop resolving when total_edges_checked is reached
            if k == total_edges_checked: break
            else: k+= 1
            e_resolve = row[0]
            ip1_r = row[1]
            ip2_r = row[2]
            l12_r = int(row[3])
            resolved = False
            print '========================\nEdge to be resolved [ ', e_resolve,' ]', ip1_r, ' -> ' , ip2_r 
            # Get tuples pointing at the same edge.
            tuples = get_tuples_for_edge(e_resolve,5)
            print 'number of found tuples = ', len(tuples)
            # List of lists to store ips that could replace the [ip1,*,*....,ip2] of the edge to be resolved for each tuple
            replacements_list = []
            # Loop tuples
            # dictionary to store key: ip1+ip2 to their edge id for ease of access
            ips_to_edge = {}
            for t in tuples:
                print '\ntuple: ', t, 'h1 = ', str(Tuples[t,3])[:-2], 'h2 = ', str(Tuples[t,4])[:-2]
                if str(Tuples[t,4])[:-2] == '31':
                	print 'Will not examine tuple with h2 == 31.'
                	break
                # ips that could replace the [ip1,*,*....,ip2]
                replacing_ips = []
                # add the ip1 of the current edge because it has to be a part of the replacement
                replacing_ips.append(ip1_r)
                # The index of the last non star ip found
                known_ip_index = 0
                # Loop for each ip of the edge (including stars) = l12
                h1_index = str(Tuples[t,3])[:-2]
                h1_h2_diff = 1
                for j in xrange(l12_r):
                    # print 'searched: h1 = ', h1_index, ' h2 = ', str(int(h1_index)+h1_h2_diff), ' ip1 must be = ' , replacing_ips[known_ip_index]
                    # find tuple with h1 = h1_r and h2 = h1_r + 1 initially, then both would change according to findings
                    record_tmp = c_dataset.find_one({"src": str(Tuples[t,1])[:-2], "dst": str(Tuples[t,2])[:-2], "h1": h1_index , "h2": str(int(h1_index)+h1_h2_diff)})
                    # If not found h1 stays the same but the h2 is moved one step further from h2
                    if record_tmp == None or record_tmp['ip1'] != replacing_ips[known_ip_index]:
                        replacing_ips.append('-1')
                        h1_h2_diff +=1
                        continue
                    else: # Add the replacing edge to the list and move h1_index and reset h1_h2_diff
                        # print 'replacing Edge [ ', record_tmp['edge'] ,' ]', record_tmp['ip1'], ' -> ' , record_tmp['ip2'] 
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
                	# print 'ips_to_edge', ips_to_edge
                	combine_replace(e_resolve, replacements_list, l12_r, ips_to_edge)
                	edges_resolved += 1
                	break
                else:
                	print 'possible replacing ips: ', replacing_ips
                	# Possible replacements are only added for combining if the last ip == ip2 of edge to be resolved
                	# and at least one star is replaced by an id
                	if replacing_ips[l12_r] == ip2_r and get_star_num(replacing_ips) < l12_r - 1:
                	   	replacements_list.append(replacing_ips)
                	else: print 'Excluded from combine_replace.'
            # If none of the lists has all stars resolved, call combine_replace function to union them
            if resolved == False and len(replacements_list) > 0:
				# print 'Replacements from different tuples: ', replacements_list	
				combine_replace(e_resolve, replacements_list, l12_r, ips_to_edge)
				edges_resolved += 1 
            edges_checked +=1
            print 'Edges resolved: ', edges_resolved , '/' , edges_checked
    # Update the edge_l12 with the next value to be resolved
    # Ignore edge with stars 17,18,19,20 as they do not exist
    if edge_l12 == 21:
        edge_l12 = 16
    else: edge_l12 -= 1
    print '---------------------------------------------------'
    

print 'Time elapsed %f' % (time.time() - start_time)
    
