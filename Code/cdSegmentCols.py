import os, sys
from datetime import datetime
from genericCommon import getDictFromFile
from genericCommon import carbonDateServerStartStop
from genericCommon import useCarbonDateServer
from genericCommon import getArticlePubDate

from genericCommon import getTweetIDFromStatusURI
from genericCommon import twitterGetDescendants
from genericCommon import dereferenceURI
from genericCommon import getURIHash
from genericCommon import readTextFromFile
from genericCommon import parallelTask
from genericCommon import writeTextToFile
from genericCommon import getDomain
from genericCommon import mimicBrowser

def getTweetPubDate(tweetURI, tweetHtml):

	tweetID = getTweetIDFromStatusURI(tweetURI)
	if( len(tweetID) == 0 ):
		return ''

	tweetDate = ''
	tweetDict = twitterGetDescendants(tweetHtml)
	
	if( 'tweets' not in tweetDict ):
		return ''

	if( len(tweetDict['tweets']) == 0 ):
		return ''

	tweetDict = tweetDict['tweets'][0]
	if( tweetID == tweetDict['data-tweet-id'] ):

		try:
			tweetDate = tweetDict['tweet-time'].split(' - ')[-1].strip()
			tweetDate = str(datetime.strptime(tweetDate, '%d %b %Y'))
		except:
			genericErrorInfo()

	return tweetDate


def getPubDate(uri, html, outfilename, altOutfilename):

	print('\t\turi, getPubDate:', uri)
	pubDate = ''
	
	if( html == '' ):
		html = dereferenceURI(uri, 0)

	if( uri.startswith('https://twitter.com') ):
		pubDate = getTweetPubDate(uri, html)
	else:
		print('\t\tusing article')
		pubDate = getArticlePubDate(uri=uri, html=html)
		print('\t\tpubdate:', pubDate)


	pubDate = pubDate.strip()
	if( pubDate == '' ):
		print('\t\tusing carbondate')
		pubDate = useCarbonDateServer(uri).replace('T', ' ')
		print('\t\tpubdate:', pubDate)

	writeTextToFile(outfilename, pubDate)
	if( altOutfilename != '' ):
		writeTextToFile(altOutfilename, pubDate)

	return pubDate

def getURISeg(uriDct):

	seg = {
		'short': '',
		'long': ''
	}

	if( uriDct['custom']['is-short'] ):
		if( 'long-uri' in uriDct['custom'] ):
			if( uriDct['custom']['long-uri'] != '' ):
				seg['long'] = uriDct['custom']['long-uri']
	
	if( seg['long'] == '' ):
		seg['long'] = uriDct['uri']
	else:
		seg['short'] = uriDct['uri']

	return seg


def main(segF):
	
	segment = getDictFromFile(segF)

	if( len(segment) == 0 ):
		print('\tSegment is corrupt, returning')
		return
	
	print('\ncarbon date segment():')
	prevNow = datetime.now()

	excludeDomains = ['archive.is']
	ignoreEmptyFiles = False
	cacheOnInd = -1#-1 to switch off
	
	progress = 0
	for seg in ['ss', 'ms', 'mm', 'mc']:

		if( seg == 'mc' ):
			continue
		
		jobsLst = []
		segSize = len(segment['segmented-cols'][seg])
		for i in range(segSize):
			
			carbonDateServerStartStop('start')
			uriSize = len(segment['segmented-cols'][seg][i]['uris'])
			for j in range(uriSize):

				progress += 1
				uri = segment['segmented-cols'][seg][i]['uris'][j]
				uriSeg = getURISeg(uri)

				#skip carbon dating non-relevant uris for now
				if( 'relevant' in uri ):
					if( uri['relevant'] == False ):
						continue

				dom = getDomain( uri['uri'] )
				if( dom in excludeDomains ):
					print('\texcluding:', dom)
					continue

				html = ''
				uriHash = getURIHash( uriSeg['long'] )
				htmlFile = './Caches/HTML/' + uriHash + '.html'
				outfilename = './Caches/CD/' + uriHash + '.txt'
				
				altOutfilename = ''
				if( uriSeg['short'] != '' ):
					altOutfilename = './Caches/CD/' + getURIHash( uriSeg['short'] ) + '.txt'

				if( os.path.exists(outfilename) ):
					
					pubDate = readTextFromFile(outfilename)
					pubdate = pubDate.strip()
					if( pubdate == '' ):
						if( ignoreEmptyFiles ):
							continue
					else:
						continue

				toPrint = '\tseg: ' + seg + ' ' + str(j) + ' of ' + str(uriSize) + ', ' + str(i) + ' of ' + str(segSize) + ', p: ' + str(progress) 
				#print(toPrint)
				
				if( progress < cacheOnInd ):
					print('\tskipping', progress)
					continue

				html = ''
				if( os.path.exists(htmlFile) ):
					html = readTextFromFile(htmlFile)

				keywords = {
					'uri': uriSeg['long'],
					'html': html,
					'outfilename': outfilename,
					'altOutfilename': altOutfilename
				}

				jobsLst.append({
					'func': getPubDate, 
					'args': keywords, 
					'misc': False, 
					'print': toPrint
				})
		
		
		resLst = []
		jobCount = len(jobsLst)
		if( jobCount != 0 ):
			print('jobsLst:', jobCount)
			resLst = parallelTask(jobsLst, threadCount=3)
			
	delta = datetime.now() - prevNow
	print('\tdelta seconds:', delta.seconds)

if __name__ == "__main__":
	
	print('\ncdSegment()')
	
	if( len(sys.argv) == 2 ):
		main( sys.argv[1] )
	else:
		print('\nUsage:')
		print('\t', sys.argv[0], 'segment.json')

	