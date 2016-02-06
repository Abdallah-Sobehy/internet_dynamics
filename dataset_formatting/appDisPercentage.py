# authors: Abdallah Sobehy and Fred Aklamanu
# description: This script finds the percentage of appearance and disappearance of edges per round ( 1..24 )

from __future__ import division
from numpy import *
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import pylab
from numpy import genfromtxt
import csv
import sys
import time

csv.field_size_limit(sys.maxsize)
start_round = 1
stop_round = 5
edgeCount = 0

start_time = time.time()
# Loads the edges.csv in memory 
Edges = genfromtxt('/home/freddyflo/Dropbox/internet_dynamics_2015/dataset/csv/Edges.csv', delimiter=';', skip_header=0, skip_footer=0)
print "Size of the Edges (rows, #attributes) ", Edges.shape

f = open('percentageMetrics20.csv', "w")
f.write('start;end_round;app%;disapp%\n')
# Load the data set (wine). Variable data stores the final data (178 x 13)
rnd = 0 
with open('DisAppMetrics20.csv', 'rb') as csvfile:
    next(csvfile) # To ignore the first line
    reader = csv.reader(csvfile, delimiter=';')
    for row in reader:
        rnd += 1
        edgeCount = 0
        appCount = int(row[2])
        disAppCount = int(row[3])
        print 'appCount: %d' % appCount
        print 'disAppCount %d' % disAppCount

        with open('EdgeDynamics.csv', 'rb') as csvfile1:
            next(csvfile1) # To ignore the first line
            reader1 = csv.reader(csvfile1, delimiter=',')
            i = 0
            for rec in reader1:
                if i == rnd:
                    break
                else: i+=1
                roundEdgesApp = str(rec[1]).translate(None,'[] ').split(';')
# Fetch edgeIDS and check their corresponding l12 value from Edges collection in MongoDB and accumulate as total number of edges for the round
# l12 of 31 is not considered
            for edge in roundEdgesApp:
                tmp =  int(Edges[edge,3])   # retrieves the l12 value for an edge
                if tmp != 31:
                    edgeCount += tmp
            print 'edge count : ' , edgeCount
    
        rnd_stop = rnd + 1
        appCountPer = ( appCount / edgeCount ) * 100
        disappCountPer = ( disAppCount / edgeCount ) * 100
        f.write(str(rnd) +';'+ str(rnd_stop) + ';' + str(appCountPer) + ';' + str(disappCountPer) + '\n')

print 'Time elapsed %f' % (time.time() - start_time)

