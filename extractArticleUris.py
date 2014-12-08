__author__ = 'thodrek'

import cPickle as pickle
import argparse
from EventRegistry import *

# Read input arguments
parser = argparse.ArgumentParser(description='Please use script as "python crawler.py -s <start_date> -e <end_date>. Data format should be "YYYY-MM-DD". Example: python crawler.py -l "United States" -c "Technology" -s 2014-08-16 -e 2014-09-27')
parser.add_argument('-s','--sdate',help="Specifies the start date for collecting events.",required=True)
parser.add_argument('-e','--edate',help="Specifies the end date for collecting events.",required=True)
args = vars(parser.parse_args())

print "Reading input parameters...",
out_prefix = '/scratch0/CIDRDemo/EventReg_Data/articleUris_'+args['sdate']+'_'+args['edate']
start_date = datetime.datetime.strptime(args['sdate'], "%Y-%m-%d").date()
end_date = datetime.datetime.strptime(args['edate'], "%Y-%m-%d").date()
print "DONE."

# Initialize event registry connection
print "Initializing event registry connection...",
er = EventRegistry(host="http://eventregistry.org",logging=False)
print "DONE."

# Initialize query
print "Initializing query...",
q = QueryArticles()
q.setDateLimit(start_date, end_date)
q.addRequestedResult(RequestArticlesUriList())
q.queryParams['lang'] = "eng"
print "DONE."

# Execute query
print "Issue query...",
res = er.execQuery(q)
if 'uriList' in res:
    print "DONE. Total number of extracted uris = ",len(res['uriList'])
else:
    print "DONE. Problem with res:"
    print res

# store output
uris_filename = out_prefix+".pkl"
pickle.dump(res,open(uris_filename,"wb"))