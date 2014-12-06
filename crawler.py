import cPickle as pickle
import argparse
from EventRegistry import *
 
# Read input arguments
parser = argparse.ArgumentParser(description='Please use script as "python crawler.py -l <location> -co <concept> -c <category> -s <start_date> -e <end_date> -o <output prefix>. Data format should be "YYYY-MM-DD". Example: python crawler.py -l "United States" -c "Technology" -s 2014-08-16 -e 2014-09-27 -o usa_tech')
parser.add_argument('-l','--loc',help="Specifies the event location.",required=False)
parser.add_argument('-co','--con',help="Specifies the event concepts.",required=False)
parser.add_argument('-c','--cat',help="Specifies the event category.",required=False)
parser.add_argument('-s','--sdate',help="Specifies the start date for collecting events.",required=True)
parser.add_argument('-e','--edate',help="Specifies the end date for collecting events.",required=True)
parser.add_argument('-o','--out',help="Specifies the output prefix.",required=True)
args = vars(parser.parse_args())
            

location = args['loc']
category = args['cat']
concept = args['con']
out_prefix = args['out']
if not out_prefix:
    out_prefix = location+'_'+args['sdate']+'_'+args['edate']
start_date = datetime.datetime.strptime(args['sdate'], "%Y-%m-%d").date()
end_date = datetime.datetime.strptime(args['edate'], "%Y-%m-%d").date()

# Initialize event registry connection
er = EventRegistry(host="http://eventregistry.org",logging=False)

# Create query to extract the URIs of all related events
q=QueryEvents()
if concept:
	q.addConcept(er.getConceptUri(concept))
if category:
	q.addCategory(er.getCategoryUri(category))
q.addRequestedResult(RequestEventsUriList())
if location:
	q.addLocation(er.getLocationUri(location))
q.setDateLimit(start_date, end_date)

# Execute query
print "Retrieving event Uris..."
eventURIs = er.execQuery(q)
print "Done."


# Iterate over event URIs and extract article list
print "Creating event to articles map"
eventToArticleMap = {}
events_checked = 0.0
total_entries = len(eventURIs['uriList'])
total_articles = []
for eURI in eventURIs['uriList']:
    # Execute query to extract article list for event uri
    qEv = QueryEvent(eURI)
    qEv.addRequestedResult(RequestEventArticleUris(lang=["eng"]))
    eventRes = er.execQuery(qEv)
    # Store result
    eventToArticleMap[eURI] = eventRes['articleUris']
    total_articles.extend(eventRes['articleUris'])
    # Update progress bar
    events_checked += 1.0
    progress = events_checked*100.0/float(total_entries)
    sys.stdout.write("Event information extraction progress: %10.2f%% (%d out of %d)   \r" % (progress,events_checked,total_entries))
    sys.stdout.flush()
print "\n"
# Store output dictionary to pickle file
eventToArticleMap_filename = out_prefix+"_event2articleMap.pkl"
pickle.dump(eventToArticleMap,open(eventToArticleMap_filename,"wb"))
print "Done."

# Iterate over article URIs and extract title, body, date, time, sourceUri, sourceTitle,
# isDuplicate. Also construct an article duplicates dictionary.
print "Creating article information pickle dictionary"
articleInfo = {}
articleDuplicates = {}
articles_checked = 0.0
total_entries = len(total_articles)
for aURI in total_articles:
    # Execute query to extract article information
    qAr = QueryArticle(aURI)
    qAr.addRequestedResult(RequestArticleInfo())
    qAr.addRequestedResult(RequestArticleDuplicatedArticles(count=1000))
    articleRes = er.execQuery(qAr)
    if articleRes:
        # Store article information
        articleInfo[aURI] = {}
        articleInfo[aURI]['title'] = articleRes['info']['title']
        articleInfo[aURI]['body'] = articleRes['info']['body']
        articleInfo[aURI]['date'] = articleRes['info']['date']
        articleInfo[aURI]['time'] = articleRes['info']['time']
        articleInfo[aURI]['sourceUri'] = articleRes['info']['sourceUri']
        articleInfo[aURI]['sourceTitle'] = articleRes['info']['sourceTitle']
        articleInfo[aURI]['isDuplicate'] = articleRes['info']['isDuplicate']

        # Store article duplicate information
        articleDuplicates[aURI] = []
        for d in articleRes['duplicatedArticles']['results']:
            articleDuplicates[aURI].append(d['uri'])
        # Update progress bar
    articles_checked += 1.0
    progress = articles_checked*100.0/float(total_entries)
    sys.stdout.write("Article infromation extraction progress: %10.2f%% (%d out of %d)   \r" % (progress,articles_checked,total_entries))
    sys.stdout.flush()
print "\n"
# Store output dictionary to pickle file
articleInfo_filename = out_prefix+"_articleInfo.pkl"
articleDuplicates_filename = out_prefix+"_articleDuplicates.pkl"
pickle.dump(articleInfo,open(articleInfo_filename,"wb"))
pickle.dump(articleDuplicates,open(articleDuplicates_filename,"wb"))
print "Done."
