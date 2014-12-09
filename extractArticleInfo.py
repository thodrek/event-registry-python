__author__ = 'thodrek'

import cPickle as pickle
import argparse
from EventRegistry import *
import socket
import os
import sys

# Read input arguments
parser = argparse.ArgumentParser(description='Please use script as "python crawler.py -s <start_date> -e <end_date>. Data format should be "YYYY-MM-DD". Example: python crawler.py -l "United States" -c "Technology" -s 2014-08-16 -e 2014-09-27')
parser.add_argument('-s','--sdate',help="Specifies the start date for collecting events.",required=True)
parser.add_argument('-e','--edate',help="Specifies the end date for collecting events.",required=True)
args = vars(parser.parse_args())

print "Reading input parameters...",
if socket.gethostname() == 'balos.umiacs.umd.edu':
    out_ev_prefix = '/scratch0/CIDRDemo/EventReg_Data/eventInfo_'+args['sdate']+'_'+args['edate']
    out_art_prefix = '/scratch0/CIDRDemo/EventReg_Data/articleInfo_'+args['sdate']+'_'+args['edate']
else:
    out_ev_prefix = '/tmp/eventInfo_'+args['sdate']+'_'+args['edate']
    out_art_prefix = '/tmp/articleInfo_'+args['sdate']+'_'+args['edate']
start_date = datetime.datetime.strptime(args['sdate'], "%Y-%m-%d").date()
end_date = datetime.datetime.strptime(args['edate'], "%Y-%m-%d").date()
print "DONE."

# Initialize event registry connection
print "Initializing event registry connection...",
er = EventRegistry(host="http://eventregistry.org",logging=False)
print "DONE."

# Initialize query
print "Initializing query...",
q = QueryEvents()
q.setDateLimit(start_date, end_date)
q.addRequestedResult(RequestEventsUriList())
q.queryParams['lang'] = "eng"
q.queryParams['minArticlesInEvent'] = 50
print "DONE."

# Execute query
print "Issue query...",
res = er.execQuery(q)
if 'uriList' in res:
    print "DONE. Total number of extracted event uris = ",len(res['uriList'])
else:
    print "DONE. Problem with res:"
    print res
    sys.exit(-1)

# Iterate over events and extract article info
print "Retrieving articles..."
events_processed = 0.0
errors = 0.0
total_entries = len(res['uriList'])
eventInfo = {}
eventArticleInfo = {}
for eURI in res['uriList']:
    # initialize query
    qEv = QueryEvent(eURI)
    qEv.addRequestedResult(RequestEventInfo())
    qEv.addRequestedResult(RequestEventArticles(count = 50, lang=["eng"]))
    # execute query
    evInfoArticles = er.execQuery(qEv)
    if evInfoArticles:
        if 'articles' in evInfoArticles and 'info' in evInfoArticles:
            # grab event info
            eventInfo[eURI] = evInfoArticles['info']
            # grab article information
            eventArticleInfo[eURI] = evInfoArticles['articles']['results']
        else:
            print 'No articles or info in result!'
            errors += 1
    else:
        print 'NoneType returned!'
        errors += 1

    # update count and print progress
    events_processed += 1.0
    progress = events_processed*100.0/float(total_entries)
    sys.stdout.write("Article information extraction progress: %10.2f%% (%d out of %d, errors %d)   \r" % (progress,events_processed,total_entries, errors))
    sys.stdout.flush()

print "DONE."
print "Storing output to pickle dictionaries..."

info_ev_filename = out_ev_prefix+".pkl"
info_art_filename = out_art_prefix+".pkl"
pickle.dump(eventInfo,open(info_ev_filename,"wb"))
pickle.dump(eventArticleInfo,open(info_art_filename,"wb"))

# if on remote server transfer code to balos
if socket.gethostname() != 'balos.umiacs.umd.edu':
    os_command = "scp "+info_ev_filename+" thodrek@balos.umiacs.umd.edu:/scratch0/CIDRDemo/EventReg_Data"
    x = os.system(os_command)
    os_command = "scp "+info_art_filename+" thodrek@balos.umiacs.umd.edu:/scratch0/CIDRDemo/EventReg_Data"
    x = os.system(os_command)