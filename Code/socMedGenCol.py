import sys
from copy import deepcopy
from datetime import datetime

from genericCommon import createFolderAtPath
from genericCommon import dedupLinks
from genericCommon import dumpJsonToFile
from genericCommon import getDictFromFile
from genericCommon import getNowTime
from genericCommon import retryParallelTwtsExt
from genericCommon import isIsolatedTweet
from genericCommon import datetime_from_utc_to_local

def handleParlTwtCol(col, expThreadsFlag=True):
	
	finalCol = {
		'thread-cols': [], 
		'stats': {
			'thread-cols-links-dist': {},
			'thread-cols-tweet-count-dist': {},
			'thread-cols-reply-group-count-dist': {},
			'original-thread-cols-count': len(col),
			'thread-cols-removed': 0
		} 
	}

	for i in range( len(col) ):
		
		if( 'error' in col[i] ):
			print('\thandleParlTwtCol(): error col skipping')
			continue

		if( col[i]['is-thread'] == False and expThreadsFlag ):
			print('\thandleParlTwtCol(): none thread col skipping')
			continue

		tweetCount = len(col[i]['tweets'])
		finalCol['stats']['thread-cols-tweet-count-dist'].setdefault(tweetCount, 0)
		finalCol['stats']['thread-cols-tweet-count-dist'][tweetCount] += 1
		
		linkCount = col[i]['stats']['total-links']
		finalCol['stats']['thread-cols-links-dist'].setdefault(linkCount, 0)
		finalCol['stats']['thread-cols-links-dist'][linkCount] += 1
		

		if( linkCount == 0 ):
			continue

		
		#don't include tweets without links except root tweet
		newTwtCol = [col[i]['tweets'][0]]

		if( 'reply-group' in newTwtCol[0]['extra'] ):
			replyGroupCount = len(newTwtCol[0]['extra']['reply-group'])
			finalCol['stats']['thread-cols-reply-group-count-dist'].setdefault(replyGroupCount, 0)
			finalCol['stats']['thread-cols-reply-group-count-dist'][replyGroupCount] += 1

		for j in range(1, len(col[i]['tweets'])):
			twt = col[i]['tweets'][j]

			if( len(twt['tweet-links']) == 0 ):
				continue

			newTwtCol.append(twt)

		col[i]['tweets'] = newTwtCol
		finalCol['thread-cols'].append( col[i] )

	finalCol['stats']['thread-cols-removed'] = finalCol['stats']['original-thread-cols-count'] - len(finalCol['thread-cols'])
	return finalCol

def handleLMPCol(col):

	if( len(col) == 0 ):
		return {}

	if( 'self-collection' not in col ):
		return {}
	
	threadsDedupSet = set()
	implicitThreadDedupSet = set()
	if( len(col['self-collection']) != 0 ):
		src = col['self-collection'][0]['search-uri']

	print('\tfound LMP collection')
	print('\t\tsrc:', src)
	
	newCol = []
	twitterLMPFlag = False
	creationDateTime = datetime.strptime(col['timestamp'].split('.')[0], '%Y-%m-%dT%H:%M:%S')
	creationDateTime = str(datetime_from_utc_to_local(creationDateTime))
	col = col['collection'][0]['links']

	if( src.find('https://twitter.com/') == 0 ):
		twitterLMPFlag = True

	if( twitterLMPFlag ):
		print('\t\tFound Twitter Col')
		newCol = {
			'tweets': [], 
			'stats': {'tweet-links-dist': {}}, 
			'explicit-thread-links': [],
			'possible-implicit-thread-links': []
		}

	for i in range( len(col) ):

		'''
		tmp = deepcopy( col[i] )
		for key, val in tmp.items():
			col[i]['custom'][key] = val
		
		col[i] = col[i]['custom']
		'''

		if( 'tweet-raw-data' in col[i]['custom'] ):
			
			tweetURI = col[i]['link']
			col[i] = col[i]['custom']['tweet-raw-data']

			if( 'explicit-thread' in col[i]['extra'] ):
				if( col[i]['extra']['explicit-thread'] not in threadsDedupSet ):
					newCol['explicit-thread-links'].append( col[i]['extra']['explicit-thread'] )
					threadsDedupSet.add( col[i]['extra']['explicit-thread'] )
			
			elif( isIsolatedTweet(col[i]) == False and tweetURI not in implicitThreadDedupSet ):
				#possible implicit threads from tweets extracted from the serp (https://ws-dl.cs.odu.edu/wiki/index.php/AlexanderNwala/DailyLogs#November_30)
				newCol['possible-implicit-thread-links'].append(tweetURI)
				implicitThreadDedupSet.add(tweetURI)
			
			linkCount = len(col[i]['tweet-links'])
			newCol['stats']['tweet-links-dist'].setdefault(linkCount, 0)
			newCol['stats']['tweet-links-dist'][linkCount] += 1

			#don't include tweets without links
			if( linkCount != 0 ):
				newCol['tweets'].append( col[i] )

	if( twitterLMPFlag ):
		newCol['stats']['original-tweet-count'] = len(col)
		newCol['stats']['tweets-removed'] = newCol['stats']['original-tweet-count'] - len(newCol['tweets'])
		col = newCol

	return {
		'self': src,
		'payload': col,
		'timestamp': creationDateTime
	}

def normalizeCol(col, expThreadsFlag=False):

	print('\nnormalizeCol():')
	if( len(col) == 0 ):
		return col

	metadata = {}
	explicitThreads = []
	src = ''
	if( 'collection' in col ):
		#LMP col
		col = handleLMPCol(col)
	else:
		col = handleParlTwtCol(col, expThreadsFlag)

	return col

def getThreads(col, maxCount):
	
	threads = []
	dedupSet = set()
	
	for i in range( len(col) ):
		if( 'extra' in col[i] ):
			if( 'explicit-thread' in col[i]['extra'] ):
				
				threadURI = col[i]['extra']['explicit-thread']
				
				if( threadURI not in dedupSet ):
					dedupSet.add( threadURI )
					threads.append( threadURI )

				if( len(threads) == maxCount ):
					break

	return threads

def isMemberExplitThread(twtDict):

	if( len(twtDict) == 0 ):
		return False

	if( 'in-explicit-thread' in twtDict ):
		return twtDict['in-explicit-thread']

	return False

def genFacebookCol(col, name='', inputExtraParam=None):
	
	print('\ngenFacebookCol():')

	outCol = {}
	links = []
	src = ''
	if( len(col) == 0 ):
		return {}

	if( 'payload' in col and 'self' in col ):
		src = col['self']
		col = col['payload']
	else:
		return {}

	if( inputExtraParam is None ):
		inputExtraParam = {}

	if( 'thread-type' not in inputExtraParam ):
		inputExtraParam['thread-type'] = ''

	inputExtraParam['stats'] = {}
	inputExtraParam['stats']['post-links-dist'] = {}

	for i in range( len(col) ):

		linkCount = len( col[i]['links'] )
		inputExtraParam['stats']['post-links-dist'].setdefault(linkCount, 0)
		inputExtraParam['stats']['post-links-dist'][linkCount] += 1

		for l in col[i]['links']:
			lDict = {'uri': l, 'parent': col[i]['link']}
			links.append(lDict)

	outCol['name'] = 'facebook' + name
	outCol['description'] = ''
	outCol['source'] = src
	outCol['capture-date'] = getNowTime().replace('T', ' ')
	outCol['author'] = 'Alexander Nwala'
	outCol['collection'] = links
	outCol['more-params'] = inputExtraParam

	return outCol

def normalizeTwmCol(col, source='', inputExtraParam=None):
	
	print('\ngenSerpOrThreadsCol():')

	outCol = {}
	links = []

	if( len(col) == 0 ):
		return {}

	if( inputExtraParam is None ):
		inputExtraParam = {}

	if( 'thread-type' not in inputExtraParam ):
		inputExtraParam['thread-type'] = ''

	inputExtraParam['stats'] = {}
	inputExtraParam['stats']['tweet-links-dist'] = {}
	for i in range( len(col['payload']) ):

		
		linkCount = len( col['payload'][i]['tweet-links'] )
		inputExtraParam['stats']['tweet-links-dist'].setdefault(linkCount, 0)
		inputExtraParam['stats']['tweet-links-dist'][linkCount] += 1
		
		for l in col['payload'][i]['tweet-links']:

			if( serpOrThread == 'threads' and inputExtraParam['thread-type'] == 'explicit' ):
				if( isMemberExplitThread(col['payload'][i]) == False ):
					continue

			parent = 'https://twitter.com/' + col['payload'][i]['data-screen-name'] + '/status/' + col['payload'][i]['data-tweet-id']
			lDict = {'uri': l, 'parent': parent}
			
			if( 'root-parent' in col['payload'][i] ):
				lDict['root-parent'] = col['payload'][i]['root-parent']

			links.append(lDict)

	outCol['name'] = 'twt-' + serpOrThread + '-' + name
	outCol['description'] = ''
	outCol['source'] = source
	outCol['capture-date'] = getNowTime().replace('T', ' ')
	outCol['author'] = ''
	outCol['payload'] = links
	outCol['more-params'] = inputExtraParam

	return outCol

def genExplThreadsCol(threads, config, cacheFilename, expThreadFlag=True):

	if( len(threads) == 0 ):
		return {}

	prevNow = datetime.now()

	twts = getDictFromFile( cacheFilename )
	if( len(twts) == 0 ):
		
		twts = retryParallelTwtsExt(
			threads,
			maxRetryCount=config['maxRetryCount'],
			tweetConvMaxTweetCount=config['tweetConvMaxTweetCount'],
			maxNoMoreTweetCounter=config['maxNoMoreTweetCounter'],
			chromedriverPath=config['chromedriverPath'],
			extraParams=config
		)

		if( len(twts) != 0 ):
			dumpJsonToFile( cacheFilename, twts, indentFlag=False )
	else:
		print('\ngenExplThreadsCol(): read tweets from cache:', cacheFilename)

	delta = datetime.now() - prevNow
	twts = updateCache( threads, config, cacheFilename, expThreadFlag, twts )
	twts = normalizeCol(twts, expThreadFlag)

	return twts

def updateCache(threads, config, cacheFilename, expThreadFlag, twts):

	print('\nupdateCache')

	if( len(twts) == 0 or len(threads) == 0 ):
		print('\tempty returning:', len(twts), len(threads))
		return twts

	cacheURIs = {}
	newReqURIs = []
	
	for twtCol in twts:
		cacheURIs[ twtCol['self'] ] = True

	for uri in threads:
		if( uri not in cacheURIs ):
			newReqURIs.append(uri)
	
	print('\tnew uris:', len(newReqURIs))
	if( len(newReqURIs) == 0 ):
		return twts
	print('\twill attempt updating cache')

	updateTwts = retryParallelTwtsExt(
		newReqURIs,
		maxRetryCount=config['maxRetryCount'],
		tweetConvMaxTweetCount=config['tweetConvMaxTweetCount'],
		maxNoMoreTweetCounter=config['maxNoMoreTweetCounter'],
		chromedriverPath=config['chromedriverPath'],
		extraParams=config
	)

	if( len(updateTwts) != 0 ):
		twts =  twts + updateTwts
		dumpJsonToFile( cacheFilename, twts, indentFlag=False )

	return twts

def main(inFilenamePath, config):

	print('\nmain()')
	if( len(config) == 0 ):
		return

	filename = inFilenamePath.split('/')[-1].replace('.json', '')
	col = getDictFromFile( './' + inFilenamePath )
	col, metadata = normalizeCol(col)

	genSerpOrThreadsCol(col, filename, source=metadata['source'])
	threads = getThreads(col, config['maxThreadsToExtract'])
	genThreadsCol(threads, config, filename)

if __name__ == "__main__":
	if( len(sys.argv) == 2 ):
		config = {}
		config['maxThreadsToExtract'] = -1

		config['tweetConvMaxTweetCount'] = 20
		config['maxNoMoreTweetCounter'] = 0
		config['chromedriverPath'] = '/Users/renaissanceassembly/bin/chromedriver'

		main(sys.argv[1], config)
	else:
		print('\tusage: ' + sys.argv[0] + ' <infilename>')