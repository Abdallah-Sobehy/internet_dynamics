Reference='https://iniy.org/?p=489'

#!/usr/bin/python
import csv
import time
start_time = time.time()
from optparse import OptionParser
 
# converts a array of csv-columns to a mongodb json line
def convert_csv_to_json(csv_line, csv_headings):
	json_elements = []
	for index,heading in enumerate(csv_headings):
	     json_elements.append(heading + ": \"" + unicode(csv_line[index],'UTF-8') + "\"")
         
	line = "{ " + ', '.join(json_elements) + " }"
	return line
 
# parsing the commandline options
parser = OptionParser(description="parses a csv-file and converts it to mongodb json format. The csv file has to have the column names in the first line.")
parser.add_option("-c", "--csvfile", dest="csvfile", action="store", help="input csvfile")
parser.add_option("-j", "--jsonfile", dest="jsonfile", action="store", help="json output file")
parser.add_option("-d", "--delimiter", dest="delimiter", action="store", help="csvdelimiter")
 
(options, args) = parser.parse_args()
 
# parsing and converting the csvfile
csvreader = csv.reader(open(options.csvfile, 'rb'), delimiter=options.delimiter)
column_headings = csvreader.next()
jsonfile = open(options.jsonfile, 'wb')
 
while True:
    try: 
        csv_current_line = csvreader.next()
	json_current_line = convert_csv_to_json(csv_current_line,column_headings)
	print >>jsonfile, json_current_line
 
    except csv.Error as e :
        print "Error parsing csv: %s" % e
    except StopIteration as e:
        print "=== Finished ==="
        print 'Time elapsed %f' % (time.time() - start_time )
        break
 
jsonfile.close()

