import sys
import cPickle as pickle
from EventRegistry import *
 
# Read input
if len(sys.argv) != 5:
    print 'Wrong input used. Please use script as "python crawler.py <location> <category> <start_date> <end_date> <output prefix>. Data format should be "YYYY-MM-DD". Example: python crawler.py "United States" "Technology" "2014-08-16" "2014-09-27" "usa_tech"'
    sys.exit(-1)
else:
    location = sys.argv[1]
    category = sys.argv[2]
    start_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    end_date = datetime.strptime(sys.argv[2], "%Y-%m-%d").date()
    out_prefix = sys.argv[3]

    print location, category, out_prefix

# Initialize event registry connection
er = EventRegistry(host="http://eventregistry.org",logging=False)

# Create query to extract the URIs of all related events
q=QueryEvents()
q.addCategory(er.getCategoryUri(category))
q.addRequestedResult(RequestEventsUriList())
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
total_entries = len(eventURIS['uriList'])
total_articles = []
for eURI in eventURIS['uriList']:
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