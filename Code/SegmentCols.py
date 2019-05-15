import os
from copy import deepcopy
from math import floor
from datetime import datetime

from genericCommon import getDomain
from genericCommon import getNowTime
from genericCommon import dumpJsonToFile
from genericCommon import getDictFromFile
from genericCommon import readTextFromFile
from genericCommon import genericErrorInfo
from genericCommon import datetime_from_utc_to_local
from genericCommon import getTweetLink
from genericCommon import isSameLink
from genericCommon import getDedupKeyForURI
from genericCommon import parseTweetURI
from genericCommon import parallelTask
from genericCommon import createFolderAtPath
from genericCommon import getMimeEncType
from genericCommon import naiveIsURIShort
from genericCommon import expandUrl
from genericCommon import getURIHash
from genericCommon import redditPrlGetLinksFromComment
from genericCommon import overlapFor2Sets

from NwalaUtil.GenericPlot import plotMultiBarPlot
from NwalaUtil.GenericPlot import plotStackedBarPlot
from NwalaUtil.GenericPlot import plotScatterPlot
from NwalaUtil.GenericPlot import plotScatterPlotShape
from NwalaUtil.GenericPlot import plotBoxPlot
from NwalaUtil.GenericPlot import plotCDF2

import profSegColAuth
from PrecEval import precEvalCol

import matplotlib.pyplot as plt
import matplotlib.lines as mlines


colormap = {
	'ss': 'orange',
	'mc': 'green',
	'ms': 'deepskyblue',
	'mm': 'indianred'
}

shapemap = {
	'ss': '8',
	'mc': '*',
	'ms': 'D',
	'mm': 'x'
}

linemap = {
	'ss': '-',
	'mc': '--',
	'ms': '-.',
	'mm': ':'
}

plts = {
	'link-dist-hist': {
		'gridX': 12,
		'gridY': 4,
		'plotStop': 48,
		'colormap': colormap
	},
	'link-dist-scatter-plot': {
		'gridX': 3,
		'gridY': 4,
		'plotStop': 12,
		'colormap': colormap,
		'shapemap': shapemap,
		'linemap': linemap
	}
}

fig1 = plt.figure(num=1, figsize=(10, 14), dpi=300)
fig2 = plt.figure(num=2, figsize=(10, 9), dpi=300)
fig3 = plt.figure(num=3, figsize=(10, 9), dpi=300)
fig4 = plt.figure(num=4, figsize=(10, 9), dpi=300)
fig5 = plt.figure(num=5, figsize=(10, 9), dpi=300)
fig6 = plt.figure(num=6, figsize=(10, 9), dpi=300)
fig7 = plt.figure(num=7, figsize=(10, 9), dpi=300)
fig8 = plt.figure(num=8, figsize=(10, 9), dpi=300)
fig9 = plt.figure(num=9, figsize=(10, 9), dpi=300)
fig10 = plt.figure(num=10, figsize=(10, 9), dpi=300)
fig11 = plt.figure(num=11, figsize=(10, 9), dpi=300)
fig12 = plt.figure(num=12, figsize=(10, 9), dpi=300)
fig13 = plt.figure(num=13, figsize=(10, 9), dpi=300)
fig14 = plt.figure(num=14, figsize=(10, 13), dpi=300)
fig15 = plt.figure(num=15, figsize=(10, 13), dpi=300)

def twitterGetDeg1Sub0(segCol, keyID, tweets, colSrc, dedupSet, provenance):

	verbose = False
	for tweet in tweets:
		if( len(tweet['tweet-links']) == 0 ):
			continue

		if( keyID == tweet['data-tweet-id'] ):

			prevSize = len(segCol)
			tweet['provenance'] = provenance
			tweet['thread-pos'] = 0
			twitterPopulateSeg(
				segCol, 
				None, 
				tweet, 
				colSrc,
				dedupSet
			)

			if( prevSize != len(segCol) and verbose):
				print('\t\t\tSuccess in adding deg link')
			
			return True

	return False

def twitterGetDeg1(segCol, uri, colKind, repo, colSrc, dedupSet):

	verbose = False
	uri = uri.strip()
	colKind = colKind.strip()
	uriDct = parseTweetURI(uri)
	if( len(uri) == 0 or len(colKind) == 0 or len(repo) == 0 or len(uriDct['id']) == 0 ):
		return False

	for rep in repo:
		for tweetCol in rep['tweet-links']:
			
			if( 'output' not in tweetCol ):
				continue

			if( len(tweetCol['output']) == 0 ):
				continue

			if( tweetCol['output']['self'] != uri ):
				continue

			provenance = {'parent': {'uri': tweetCol['parent']}}
			foundFlag = twitterGetDeg1Sub0( segCol, uriDct['id'], tweetCol['output']['tweets'], colSrc, dedupSet, provenance )
			if( foundFlag ):
				if( verbose ):
					print('\t\tfound expanded deg 1 twt:', uri)

				return True
	
	return False

def genericGetMimeType(uri):
	mime, enc = getMimeEncType(uri)
	if( mime is None ):
		return ''
	return mime



def redditAuthorComp(a, b):
	if( a['custom']['author'] == b['custom']['author'] ):
		return True
	else:
		return False

def facebookAuthorComp(a, b):
	if( a['custom']['author']['name'] == b['author']['name'] ):
		return True
	else:
		return False

def genericAddReplyGroup(src, authorCompFunc):

	if( 'payload' not in src ):
		print('\tredditAddReplyGroup(): returning no payload')
		return

	for i in range( len(src['payload']) ):
		
		seg = src['payload'][i]
		if( 'expanded-comments' not in seg['custom'] ):
			continue			

		comments = seg['custom']['expanded-comments']['comments']
		if( len(comments) == 0 ):
			continue

		if( authorCompFunc(seg, comments[0]) == False ):
			continue


		seg['custom']['reply-group'] = []
		for j in range(len(comments)):
			
			if( authorCompFunc(seg, comments[j]) == False ):
				break
			tmp = {
				'id': comments[j]['id'],
				'pos': j
			}
			seg['custom']['reply-group'].append(tmp)


def redditGetPstDets(pst, src, degree):
	if( len(pst) == 0 ):
		return {}

	tmp = {}
	try:
		tmp['id'] = pst['id']
		tmp['parent-id'] = pst['parent-id']
		tmp['author'] = pst['custom']['author']
		tmp['uri'] = pst['custom']['permalink']
		tmp['src'] = src
		tmp['provenance'] = pst['provenance']
		tmp['thread-pos'] = pst['thread-pos']
		

		tmp['creation-date'] = datetime.strptime(pst['pub-datetime'], '%Y-%m-%dT%H:%M:%S')
		tmp['creation-date'] = str( datetime_from_utc_to_local(tmp['creation-date']) )
		
		tmp['substitute-text'] = pst['title'] + '\n' + pst['text']
		tmp['substitute-text'] = tmp['substitute-text'].strip()
		tmp['custom'] = {}
		if( 'custom' in pst['custom'] ):
			tmp['custom'] = pst['custom']['custom']

		if( len(pst['link']) == 0 ):
			uriType = 'comment'
		else:
			uriType = getGenericURIType( pst['link'], pst['custom']['permalink'], 'reddit.com', degree=degree )
		
			if( uriType == 'internal-self' ):
				uriType = 'self'
			elif( uriType == 'internal-degree-' + str(degree) ):
				uriType = 'permalink'
			else:
				#external
				indx = tmp['uri'].find('://redd.it/')
				if( indx == 4 or indx == 5 ):
					uriType = 'external-shortlink'

		tmp['post-uri-type'] = uriType
	except:
		genericErrorInfo()

	return tmp

def twitterGetPstDets(pst, src):
	if( len(pst) == 0 ):
		return {}

	tmp = {}
	try:
		tmp['id'] = pst['data-tweet-id']
		tmp['parent-id'] = ''

		if( tmp['id'] != pst['data-conversation-id'] ):
			tmp['parent-id'] = pst['data-conversation-id']

		tmp['author'] = pst['data-screen-name']
		tmp['uri'] = getTweetLink(tmp['author'], tmp['id'])
		tmp['src'] = src
		tmp['creation-date'] = ''
		tmp['provenance'] = pst['provenance']
		tmp['thread-pos'] = pst['thread-pos']

		tmp['substitute-text'] = pst['tweet-text'].strip()
		try:
			#tweet-time sample: 6:19 PM - 2 Apr 2014
			tmp['creation-date'] = datetime.strptime(pst['tweet-time'], '%I:%M %p - %d %b %Y')
			tmp['creation-date'] = str( tmp['creation-date'].strftime('%Y-%m-%d %X') )
		except:
			genericErrorInfo()
	except:
		genericErrorInfo()
		print('\t\tError dets:', src, pst['data-tweet-id'])

	return tmp

def facebookGetPstDets(pst, src):
	if( len(pst) == 0 ):
		return {}

	tmp = {}
	try:
		tmp['id'] = pst['id']
		tmp['parent-id'] = ''

		if( 'parent' in pst ):
			tmp['parent-id'] = pst['parent']['id']
		
		tmp['author'] = pst['author']['name']
		tmp['uri'] = pst['time']['uri']
		tmp['src'] = src
		tmp['provenance'] = pst['provenance']
		tmp['thread-pos'] = pst['thread-pos']
		tmp['creation-date'] = ''

		tmp['substitute-text'] = pst['text'].strip()
		try:
			#time sample: epoch
			pst['time']['utime'] = int(pst['time']['utime'])
			tmp['creation-date'] = str(datetime.fromtimestamp(pst['time']['utime']))
		except:
			genericErrorInfo()
			tmp['creation-date'] = pst['time']['text']
	except:
		genericErrorInfo()

	return tmp

def sutoriGetPstDets(pst, src):
	if( len(pst) == 0 ):
		return {}

	tmp = {}
	try:
		tmp['id'] = ''
		tmp['parent-id'] = ''
		tmp['author'] = pst['author']
		tmp['uri'] = pst['story']
		tmp['src'] = src
		tmp['creation-date'] = ''
		tmp['provenance'] = pst['provenance']

		tmp['substitute-text'] = pst['title'].strip()
	except:
		genericErrorInfo()

	return tmp

def scoopitGetPstDets(pst, src):

	if( len(pst) == 0 ):
		return {}

	tmp = {}
	try:
	
		tmp['id'] = ''
		tmp['parent-id'] = ''
		tmp['author'] = pst['scooped-by']['name']
		tmp['uri'] = pst['scooped-onto']['uri']
		tmp['src'] = src
		tmp['creation-date'] = ''
		tmp['provenance'] = pst['provenance']

		tmp['substitute-text'] = pst['title'].strip()

		#date format example: "August 24, 11:16 AM" or "January 1, 2016 3:08 PM"
		for form in ['%B %d, %I:%M %p', '%B %d, %Y %I:%M %p']:
			try:
				tmp['creation-date'] = datetime.strptime(pst['creation-date'].strip(), form)
				tmp['creation-date'] = tmp['creation-date'].replace(year=datetime.now().year)
				tmp['creation-date'] = str(tmp['creation-date'])
				break
			except:
				pass
	except:
		genericErrorInfo()
	
	return tmp

def scoopitMSGetPstDets(pst, src):

	if( len(pst) == 0 ):
		return {}

	tmp = {}
	try:

		tmp['id'] = ''
		tmp['parent-id'] = ''
		tmp['author'] = pst['curated-by']['name']
		tmp['uri'] = pst['uri']
		tmp['src'] = src
		tmp['creation-date'] = ''
		tmp['provenance'] = pst['provenance']
		#date format example: "August 24, 11:16 AM" or "January 1, 2016 3:08 PM"
		for form in ['%B %d, %I:%M %p', '%B %d, %Y %I:%M %p']:
			try:
				tmp['creation-date'] = datetime.strptime(pst['creation-date'].strip(), form)
				tmp['creation-date'] = tmp['creation-date'].replace(year=datetime.now().year)
				tmp['creation-date'] = str(tmp['creation-date'])
				break
			except:
				pass
	except:
		genericErrorInfo()
	
	return tmp


def getGenericURIType(uri, permalink, chkDomain, degree=1):

	uri = uri.strip()
	if( len(uri) == 0 ):
		return ''

	uriType = ''
	if( getDomain(uri, includeSubdomain=False) == chkDomain ):
		if( isSameLink(permalink, uri) ):
			uriType = 'internal-self'
		else:
			uriType = 'internal-degree-' + str(degree)
	else:
		uriType = 'external'

	return uriType

def redditGetURIType(uri, permalink, chkDomain, degree=1):
	uriType = getGenericURIType( uri, permalink, chkDomain, degree=degree )
	if( uriType == 'external' ):
		#external
		indx = uri.find('://redd.it/')
		if( indx == 4 or indx == 5 ):
			uriType = 'external-shortlink'
	return uriType

def redditAddLinksToSegCol(singleSegCol, links, pstDet, degree, dedupSet):

	for l in links:
		l = l.strip()

		if( len(l) == 0 ):
			continue

		uriKey = getDedupKeyForURI(l)
		if( uriKey in dedupSet ):
			continue

		dedupSet.add(uriKey)

		uriType = redditGetURIType(l, pstDet['uri'], 'reddit.com', degree=degree)
		uriDct = {
			'uri': l, 
			'post-details': pstDet, 
			'custom': {'uri-type': 'extra-link-' + uriType, 'is-short': naiveIsURIShort(l)}
		}
		singleSegCol['uris'].append( uriDct )

def redditAddRootCol(segCol, post, colSrc, singleSegIndx=-1, extraParams=None):

	if( extraParams == None ):
		extraParams = {}
	
	extraParams.setdefault('dedupSet', set())
	extraParams.setdefault('degree', 1)
	
	pstDet = redditGetPstDets(post, colSrc, degree=extraParams['degree'])
	uriType = redditGetURIType( post['link'], post['custom']['permalink'], 'reddit.com', degree=extraParams['degree'] )

	post['link'] = post['link'].strip()
	root = {
		'uri': post['link'], 
		'post-details': pstDet, 
		'custom': {'uri-type': uriType, 'is-short': naiveIsURIShort(post['link'])}
	}
	
	if( singleSegIndx != -1 and singleSegIndx < len(segCol) ):
		singleSegCol = segCol[singleSegIndx]
	else:
		singleSegCol = {}
	
	singleSegCol.setdefault('uris', [])
	singleSegCol.setdefault('stats', {})
	
	#uriType options: internal-self, internal-degree-1, external
	uriKey = getDedupKeyForURI( root['uri'] )
	if( uriType != 'internal-self' and uriKey not in extraParams['dedupSet'] ):

		extraParams['dedupSet'].add(uriKey)
		#don't add self link, but may add links embeded in the post
		if( len(root['uri']) != 0 ):
			#addition into segment also takes place in redditAddLinksToSegCol
			singleSegCol['uris'].append(root)


	redditAddLinksToSegCol(singleSegCol, post['links'], pstDet, extraParams['degree'], extraParams['dedupSet'])
	if( len(singleSegCol['uris']) != 0 and singleSegIndx == -1 ):
		#case where root post is added for the first time, this case is false (singleSegIndx != -1)
		#when a uri from a comment is to be added to singleSegCol which is already in segCol
		singleSegIndx = len(segCol)
		segCol.append( singleSegCol )

	singleSegCol['stats']['uri-count'] = len(singleSegCol['uris'])

	return singleSegIndx

def twitterPopulateSeg(container, localDeg1Col, tweet, colSrc, dedupSet):

	pstDet = twitterGetPstDets(tweet, colSrc)
	pstDet['post-uri-type'] = 'tweet'

	for uri in tweet['tweet-links']:

		uriKey = getDedupKeyForURI(uri)
		if( uriKey in dedupSet ):
			continue

		dedupSet.add(uriKey)
		uriType = getGenericURIType( uri, pstDet['uri'], 'twitter.com' )
		uriDct = {
			'uri': uri, 
			'post-details': pstDet, 
			'custom': {'uri-type': uriType, 'is-short': naiveIsURIShort(uri)}
		}
		
		#uriType options: internal-self, internal-degree-1, external
		if( uriType == 'internal-degree-1' ):
			if( localDeg1Col is not None ):
				localDeg1Col.append(uriDct)
		elif( uriType == 'external' ):
			#don't add self link, but may add links embeded in the post
			container.append(uriDct)

def twitterAddRootCol(segCol, post, deg1Container, colSrc, singleSegIndx=-1, extraParams=None):

	verbose = False
	if( extraParams == None ):
		extraParams = {}

	if( 'deg1ColRepo' not in extraParams ):
		extraParams['deg1ColRepo'] = {}

	if( 'colKind' not in extraParams ):
		extraParams['colKind'] = ''

	if( 'dedupSet' not in extraParams ):
		extraParams['dedupSet'] = set()

	
	if( singleSegIndx != -1 and singleSegIndx < len(segCol) ):
		singleSegCol = segCol[singleSegIndx]
	else:
		singleSegCol = {}
	

	singleSegCol.setdefault('uris', [])
	singleSegCol.setdefault('stats', {})
	#addition into seg takes place in twitterPopulateSeg and twitterGetDeg1 for deg 1 links
	localDeg1Col = []
	twitterPopulateSeg(
		singleSegCol['uris'], 
		localDeg1Col, 
		post, 
		colSrc,
		extraParams['dedupSet']
	)

	#handle degree-1 col - start
	#crucial this block is here
	for deg1UriDct in localDeg1Col:
		if( verbose ):
			print('\t', colSrc + '-' + extraParams['colKind'] + ': try add deg1 links frm:', deg1UriDct['uri'])

		deg1Container.append( deg1UriDct )
		found = twitterGetDeg1(singleSegCol['uris'], deg1UriDct['uri'], extraParams['colKind'], extraParams['deg1ColRepo'], colSrc, extraParams['dedupSet'])
		
		#if( found ):
		#	print('\tsucceed in extracting expanded deg1: consider not adding deg1UriDct into deg1Container when provenance implemented')
	#handle degree-1 col - end
	
	if( len(singleSegCol['uris']) != 0 and singleSegIndx == -1 ):
		#case where root post is added for the first time, this case is false (singleSegIndx != -1)
		#when a uri from a comment is to be added to singleSegCol which is already in segCol
		singleSegIndx = len(segCol)
		segCol.append( singleSegCol )


	singleSegCol['stats']['uri-count'] = len(singleSegCol['uris'])
	return singleSegIndx

def genericAddRootCol(segCol, post, deg1Container, colSrc, extraParams):

	singleSegIndx = -1
	pstURIType = ''

	if( 'pstDetFunc' not in extraParams ):
		print('\n\tgenericAddRootCol():missing pstDetFunc, noop')
		return -1

	if( 'srcDomain' not in extraParams ):
		print('\n\tgenericAddRootCol():missing srcDomain, noop')
		return -1

	if( 'pstURIType' in extraParams ):
		pstURIType = extraParams['pstURIType']

	if( 'dedupSet' not in extraParams ):
		extraParams['dedupSet'] = set()


	pstDet = extraParams['pstDetFunc']( post, colSrc )
	pstDet['post-uri-type'] = pstURIType
	
	if( 'singleSegIndx' in extraParams ):
		singleSegIndx = extraParams['singleSegIndx']

	
	if( singleSegIndx != -1 and singleSegIndx < len(segCol) ):
		singleSegCol = segCol[singleSegIndx]
	else:
		singleSegCol = {}
	
	singleSegCol.setdefault('uris', [])
	singleSegCol.setdefault('stats', {})
	
	for uri in post['links']:

		title = ''
		if( isinstance(uri, dict) ):
			if( 'title' in uri ):
				title = uri['title']

			uri = uri['uri']


		uriKey = getDedupKeyForURI(uri)
		if( uriKey in extraParams['dedupSet'] ):
			continue

		extraParams['dedupSet'].add(uriKey)
		uriType = getGenericURIType( uri, pstDet['uri'], extraParams['srcDomain'] )
		uriDct = {
			'uri': uri, 
			'title': title, 
			'post-details': pstDet, 
			'custom': {'uri-type': uriType, 'is-short': naiveIsURIShort(uri)}
		}
		
		#uriType options: internal-self, internal-degree-1, external
		if( uriType == 'internal-degree-1' ):
			deg1Container.append(uriDct)
		elif( uriType == 'external' ):
			#don't add self link, but may add links embeded in the post
			singleSegCol['uris'].append(uriDct)
	
	
	if( len(singleSegCol['uris']) != 0 and singleSegIndx == -1 ):
		#case where root post is added for the first time, this case is false (singleSegIndx != -1)
		#when a uri from a comment is to be added to singleSegCol which is already in segCol
		singleSegIndx = len(segCol)
		segCol.append( singleSegCol )

	singleSegCol['stats']['uri-count'] = len(singleSegCol['uris'])
	return singleSegIndx



def redditSSColAdd(src, container, colSrc, verbose=False):
	
	colKind = 'ss'
	extraParams = {}
	extraParams['dedupSet'] = set()
	for post in src['payload']:
		
		if( verbose ):
			print( '\tparent:', post['custom']['permalink'] )
			print( '\tlink:', post['link'] )
			print( '\tkind:', post['kind'] )
			print('\tseg:', post)
		
		post['provenance'] = {'parent': {'uri': src['self']}}
		post['thread-pos'] = 0
		singleSegIndx = redditAddRootCol(
			container['segmented-cols'][colKind], 
			post,
			colSrc,
			extraParams=extraParams
		)

		#singleSegIndx indicates an addition of link(s) to a single segment
		if( singleSegIndx != -1 ):
			container['segmented-cols'][colKind][singleSegIndx]['stats']['total-posts'] = 1

		if( verbose ):
			print('\texternal link/link count', len(post['links']))
			print('\tcom count:', post['stats']['comment-count'])
			print()

def twitterSSColAdd(src, container, colSrc, deg1ColRepo, provenance):

	verbose = False

	if( verbose ):
		print('\ntwitterSSColAdd():')
		print('\ttweet count:', len(src['payload']['tweets']) )

	colKind = 'ss'

	extraParams = {}
	extraParams['colKind'] = colKind
	extraParams['deg1ColRepo'] = deg1ColRepo
	extraParams['dedupSet'] = set()
	for post in src['payload']['tweets']:
		
		if( len(post['tweet-links']) == 0 ):
			continue

		post['thread-pos'] = 0
		post['provenance'] = provenance
		singleSegIndx = twitterAddRootCol(
			container['segmented-cols'][colKind], 
			post, 
			container['segmented-cols']['degree-1'][colKind], 
			colSrc,
			extraParams=extraParams
		)

		if( singleSegIndx != -1 ):
			container['segmented-cols'][colKind][singleSegIndx]['stats']['total-posts'] = 1
		
		
	if( verbose ):
		print()

def facebookSSColAdd(src, container, colSrc, provenance):
	
	colKind = 'ss'
	
	extraParams = {}
	extraParams['colKind'] = colKind
	extraParams['pstDetFunc'] = facebookGetPstDets
	extraParams['srcDomain'] = 'facebook.com'
	extraParams['pstURIType'] = 'post'
	extraParams['dedupSet'] = set()

	for i in range(len(src['payload'])):
		
		post = src['payload'][i]['custom']
		post['thread-pos'] = 0

		if( len(post['links']) == 0 ):
			continue

		post['provenance'] = provenance
		singleSegIndx = genericAddRootCol(
			container['segmented-cols'][colKind], 
			post, 
			container['segmented-cols']['degree-1'][colKind], 
			colSrc,
			extraParams=extraParams
		)

		if( singleSegIndx != -1 ):
			container['segmented-cols'][colKind][singleSegIndx]['stats']['total-posts'] = 1

def sutoriSSColAdd(src, container, colSrc):
	
	colKind = 'ss'
	extraParams = {}
	extraParams['pstDetFunc'] = sutoriGetPstDets
	extraParams['srcDomain'] = 'sutori.com'
	extraParams['pstURIType'] = 'story'
	extraParams['dedupSet'] = set()
	for i in range(len(src['payload'])):
		
		post = src['payload'][i]
		if( len(post['links']) == 0 ):
			continue

		post['provenance'] = {'parent': {'uri': src['self']}}
		singleSegIndx = genericAddRootCol(
			container['segmented-cols'][colKind], 
			post, 
			container['segmented-cols']['degree-1'][colKind], 
			colSrc,
			extraParams=extraParams
		)

		if( singleSegIndx != -1 ):
			container['segmented-cols'][colKind][singleSegIndx]['stats']['total-posts'] = 1

def scoopitSSColAdd(src, container, colSrc):
	
	colKind = 'ss'
	extraParams = {}
	extraParams['pstDetFunc'] = scoopitGetPstDets
	extraParams['srcDomain'] = 'scoop.it'
	extraParams['pstURIType'] = 'scoop'
	extraParams['dedupSet'] = set()
	for i in range(len(src['scoops']['payload'])):
		
		post = src['scoops']['payload'][i]
		post['links'] = [{
			'uri': post['uri'], 
			'title': post['title']
		}]

		post['provenance'] = {'parent': {'uri': src['scoops']['self']}}
		singleSegIndx = genericAddRootCol(
			container['segmented-cols'][colKind], 
			post, 
			container['segmented-cols']['degree-1'][colKind], 
			colSrc,
			extraParams=extraParams
		)

		if( singleSegIndx != -1 ):
			container['segmented-cols'][colKind][singleSegIndx]['stats']['total-posts'] = 1

		del post['links']



def redditMSColAdd(src, container, colSrc, verbose=False):

	colKind = 'ms'
	extraParams = {}
	extraParams['dedupSet'] = set()
	

	for post in src['payload']:
		
		post['thread-pos'] = 0
		if( 'expanded-comments' not in post['custom'] ):
			continue

		if( 'reply-group' not in post['custom'] ):
			continue

		if( verbose ):
			print( '\tparent:', post['custom']['permalink'] )
			print( '\tlink:', post['link'] )
			print( '\tkind:', post['kind'] )
			print( '\tid/parent-id/author', post['id'], post['parent-id'], post['custom']['author'] )
		
		containerPrevSize = len(container['segmented-cols'][colKind])
		post['provenance'] = {'parent': {'uri': src['self']}}
		singleSegIndx = redditAddRootCol(
			container['segmented-cols'][colKind], 
			post,
			colSrc,
			extraParams=extraParams
		)

		if( verbose ):
			print('\tseg:', post)
			print('\tadd reply group links:', len(post['custom']['reply-group']))

		for i in range(len(post['custom']['reply-group'])):
			
			memb = post['custom']['reply-group'][i]
			indx = memb['pos']#this pos is position of member in reply-group list
			if( verbose ):
				print('\t\tmem:', memb)
			
			memb = post['custom']['expanded-comments']['comments'][indx]
			if( len(memb['links']) == 0 ):
				continue

			if( verbose ):
				print('\t\tto add mem:', memb)
			
			if( memb['parent-id'] == '' ):
				memb['provenance'] = {'parent': {'uri': src['self']}}
			else:
				memb['provenance'] = {
					'parent': {
						'uri': memb['custom']['permalink'].replace('/' + memb['id'] + '/', '/' + memb['parent-id'].split('_')[-1] + '/')
					}
				}

			memb['thread-pos'] = i+1
			singleSegIndx = redditAddRootCol(
				container['segmented-cols'][colKind], 
				memb, 
				colSrc + '-comments',
				singleSegIndx=singleSegIndx
			)

		if( containerPrevSize != len(container['segmented-cols'][colKind]) ):
			#count addition of root post and maximum possible elements added to singleSegCol
			container['segmented-cols'][colKind][-1]['stats']['total-posts'] = 1 + len(post['custom']['reply-group'])

		if( verbose ):
			print()

def twitterMSColAdd(src, container, colSrc, threadType, deg1ColRepo, provenance):

	verbose = False
	if( verbose ):
		print('\ntwitterMSColAdd():')
		print('\tthread count:', len(src['thread-cols']) )

	colKind = 'ms'
	
	extraParams = {}
	extraParams['colKind'] = colKind
	extraParams['deg1ColRepo'] = deg1ColRepo
	extraParams['dedupSet'] = set()

	for threadCol in src['thread-cols']:
		
		if( 'tweets' in threadCol ):
			if( len(threadCol['tweets']) != 0 ):
				threadCol['tweets'][0]['thread-pos'] = 0

		if( threadCol['is-thread'] == False ):
			continue

		if( threadCol['stats']['total-links'] == 0 ):
			continue

		containerPrevSize = len(container['segmented-cols'][colKind])
		threadCol['tweets'][0]['provenance'] = {
			'parent': {
				'uri': threadCol['self'],
				'parent': provenance
			}
		}
		singleSegIndx = twitterAddRootCol(
			container['segmented-cols'][colKind], 
			threadCol['tweets'][0], 
			container['segmented-cols']['degree-1'][colKind], 
			colSrc,
			extraParams=extraParams
		)
		
		totalTweetCount = len(threadCol['tweets'])
		if( verbose ):
			print('\t\ttweet count:', totalTweetCount )
		for i in range(1, totalTweetCount):
			
			memb = threadCol['tweets'][i]
			memb['thread-pos'] = memb['pos']#use pos since intermediate tweets might be absent
			if( memb['extra']['in-' + threadType] == False ):
				continue
			
			if( len(memb['tweet-links']) == 0 ):
				continue

			memb['provenance'] = {
				'parent': {
					'uri': threadCol['self'],
					'parent': provenance
				}
			}
			singleSegIndx = twitterAddRootCol(
				container['segmented-cols'][colKind], 
				memb, 
				container['segmented-cols']['degree-1'][colKind], 
				colSrc + '-' + threadType,
				singleSegIndx=singleSegIndx,
				extraParams=extraParams
			)

		if( containerPrevSize != len(container['segmented-cols'][colKind]) ):
			#count addition of root post and maximum possible elements added to singleSegCol
			#note that tweet without links have been removed
			container['segmented-cols'][colKind][-1]['stats']['total-posts'] = 1 + len(threadCol['tweets'][0]['extra']['reply-group'])
	
	if( verbose ):
		print()

def facebookMSColAdd(src, container, colSrc, provenance):

	colKind = 'ms'
	extraParams = {}
	extraParams['pstDetFunc'] = facebookGetPstDets
	extraParams['srcDomain'] = 'facebook.com'
	extraParams['pstURIType'] = 'post'
	extraParams['dedupSet'] = set()
	for i in range(len(src['payload'])):
		
		
		if( 'custom' in src['payload'][i] ):
			post = src['payload'][i]['custom']
		else:
			post = src['payload'][i]

		post['thread-pos'] = 0
		if( 'expanded-comments' not in post ):
			continue

		if( 'reply-group' not in post ):
			continue

		containerPrevSize = len(container['segmented-cols'][colKind])
		post['provenance'] = provenance
		extraParams['singleSegIndx'] = genericAddRootCol(
			container['segmented-cols'][colKind], 
			post, 
			container['segmented-cols']['degree-1'][colKind], 
			colSrc,
			extraParams=extraParams
		)

		for j in range( len(post['reply-group']) ):
			
			memb = post['reply-group'][j]
			indx = memb['pos']#position is series of comments
			
			memb = post['expanded-comments']['comments'][indx]
			memb['thread-pos'] = j+1
			
			if( len(memb['links']) == 0 ):
				continue

			memb['provenance'] = {
				'parent': {
					'uri': post['expanded-comments']['uri'],
					'parent': provenance
				}
			}
			extraParams['singleSegIndx'] = genericAddRootCol(
				container['segmented-cols'][colKind], 
				memb, 
				container['segmented-cols']['degree-1'][colKind], 
				colSrc + '-comments',
				extraParams=extraParams
			)

		if( containerPrevSize != len(container['segmented-cols'][colKind]) ):
			#count addition of root post and maximum possible elements added to singleSegCol
			container['segmented-cols'][colKind][-1]['stats']['total-posts'] = 1 + len(post['reply-group'])

def scoopitMSColAdd(src, container, colSrc):

	print('\tscoopitMSColAdd()')

	colKind = 'ms'
	extraParams = {}
	extraParams['pstDetFunc'] = scoopitMSGetPstDets
	extraParams['srcDomain'] = 'scoop.it'
	extraParams['pstURIType'] = 'topic'
	extraParams['dedupSet'] = set()
	for i in range(len(src['topics']['payload'])):
		
		posts = deepcopy( src['topics']['payload'][i] )

		if( len(posts['posts']) == 0 ):
			continue
		
		posts['links'] = posts.pop('posts')
		
		posts['provenance'] = {'parent': {'uri': src['topics']['self']}}
		containerPrevSize = len(container['segmented-cols'][colKind])
		genericAddRootCol(
			container['segmented-cols'][colKind], 
			posts, 
			container['segmented-cols']['degree-1'][colKind], 
			colSrc,
			extraParams=extraParams
		)

		if( containerPrevSize != len(container['segmented-cols'][colKind]) ):
			container['segmented-cols'][colKind][-1]['stats']['total-posts'] = len(posts['links'])



def redditMMColAdd(src, container, colSrc, verbose=False):

	colKind = 'mm'
	extraParams = {}
	extraParams['dedupSet'] = set()
	for post in src['payload']:
		
		post['thread-pos'] = 0
		if( 'expanded-comments' not in post['custom'] ):
			continue

		if( verbose ):
			print( '\tparent:', post['custom']['permalink'] )
			print( '\tlink:', post['link'] )
			print( '\tkind:', post['kind'] )
			print( '\tid/parent-id/author', post['id'], post['parent-id'], post['custom']['author'] )
		
		post['provenance'] = {'parent': {'uri': src['self']}}
		
		containerPrevSize = len(container['segmented-cols'][colKind])
		singleSegIndx = redditAddRootCol(
			container['segmented-cols'][colKind], 
			post, 
			colSrc,
			extraParams=extraParams
		)

		if( verbose ):
			print('\tseg:', post)
			print('\tadd comment links:', len(post['custom']['expanded-comments']))


		for i in range(len(post['custom']['expanded-comments']['comments'])):

			com = post['custom']['expanded-comments']['comments'][i]
			com['thread-pos'] = i+1
			if( len(com['links']) == 0 ):
				continue


			if( com['parent-id'] == '' ):
				com['provenance'] = {'parent': {'uri': src['self']}}
			else:
				com['provenance'] = {
					'parent': {
						'uri': com['custom']['permalink'].replace('/' + com['id'] + '/', '/' + com['parent-id'].split('_')[-1] + '/')
					}
				}

			singleSegIndx = redditAddRootCol(
				container['segmented-cols'][colKind], 
				com, 
				colSrc + '-comments',
				singleSegIndx=singleSegIndx
			)

			if( verbose ):
				print('\t\tto add mem:', com)

		
		if( containerPrevSize != len(container['segmented-cols'][colKind]) ):
			#count addition of root post and maximum possible elements added to singleSegCol
			container['segmented-cols'][colKind][-1]['stats']['total-posts'] = 1 + len(post['custom']['expanded-comments']['comments'])
		
		if( verbose ):
			print()

def twitterMMColAdd(src, container, colSrc, threadType, deg1ColRepo, provenance):

	verbose = False

	if( verbose ):
		print('\ntwitterMMColAdd():')
		print('\tthread count:', len(src['thread-cols']) )

	colKind = 'mm'
	#this function is essentially twitterMS without check for 
	#thread and if tweet is a member of explicit, implicit class

	extraParams = {}
	extraParams['colKind'] = colKind
	extraParams['deg1ColRepo'] = deg1ColRepo
	extraParams['dedupSet'] = set()

	for threadCol in src['thread-cols']:
		
		if( 'tweets' in threadCol ):
			if( len(threadCol['tweets']) != 0 ):
				threadCol['tweets'][0]['thread-pos'] = 0

		if( threadCol['stats']['total-links'] == 0 ):
			continue

		containerPrevSize = len(container['segmented-cols'][colKind])
		threadCol['tweets'][0]['provenance'] = {
			'parent': {
				'uri': threadCol['self'],
				'parent': provenance
			}
		}
		singleSegIndx = twitterAddRootCol(
			container['segmented-cols'][colKind], 
			threadCol['tweets'][0], 
			container['segmented-cols']['degree-1'][colKind], 
			colSrc,
			extraParams=extraParams
		)
		
		totalTweetCount = len(threadCol['tweets'])
		for i in range(1, totalTweetCount):
			
			memb = threadCol['tweets'][i]
			memb['thread-pos'] = memb['pos']
			if( len(memb['tweet-links']) == 0 ):
				continue

			memb['provenance'] = {
				'parent': {
					'uri': threadCol['self'],
					'parent': provenance
				}
			}
			singleSegIndx = twitterAddRootCol(
				container['segmented-cols'][colKind], 
				memb, 
				container['segmented-cols']['degree-1'][colKind], 
				colSrc + '-' + threadType,
				singleSegIndx=singleSegIndx,
				extraParams=extraParams
			)

		if( containerPrevSize != len(container['segmented-cols'][colKind]) ):
			#count addition of root post and maximum possible elements added to singleSegCol
			#note that tweet without links have been removed so use total tweets
			container['segmented-cols'][colKind][-1]['stats']['total-posts'] = threadCol['stats']['total-tweets']

def twitterMomentsMMColAdd(src, container, colSrc, deg1ColRepo, provenance):

	verbose = False

	if( verbose ):
		print('\ntwitterMomentsMMColAdd():')
		print('\ttweet count:', len(src['payload']) )

	colKind = 'mm'
	singleSegIndx = -1

	extraParams = {}
	extraParams['colKind'] = colKind
	extraParams['deg1ColRepo'] = deg1ColRepo
	extraParams['dedupSet'] = set()

	for post in src['payload']:
		
		post['thread-pos'] = 0

		if( post['tweet-links'] == 0 ):
			continue
		
		containerPrevSize = len(container['segmented-cols'][colKind])
		post['provenance'] = provenance
		singleSegIndx = twitterAddRootCol(
			container['segmented-cols'][colKind], 
			post, 
			container['segmented-cols']['degree-1'][colKind], 
			colSrc,
			singleSegIndx=singleSegIndx,
			extraParams=extraParams
		)
		
		if( containerPrevSize != len(container['segmented-cols'][colKind]) ):
			container['segmented-cols'][colKind][-1]['stats']['total-posts'] = len(src['payload'])

def facebookMMColAdd(src, container, colSrc, provenance):

	colKind = 'mm'
	extraParams = {}
	extraParams['pstDetFunc'] = facebookGetPstDets
	extraParams['srcDomain'] = 'facebook.com'
	extraParams['pstURIType'] = 'post'
	extraParams['dedupSet'] = set()
	for i in range(len(src['payload'])):
		
		if( 'custom' in src['payload'][i] ):
			post = src['payload'][i]['custom']
		else:
			post = src['payload'][i]
		
		post['thread-pos'] = 0
		if( 'expanded-comments' not in post ):
			continue
		

		containerPrevSize = len(container['segmented-cols'][colKind])
		post['provenance'] = provenance
		extraParams['singleSegIndx'] = genericAddRootCol(
			container['segmented-cols'][colKind], 
			post, 
			container['segmented-cols']['degree-1'][colKind], 
			colSrc,
			extraParams=extraParams
		)


		for j in range(len(post['expanded-comments']['comments'])):

			com = post['expanded-comments']['comments'][j]
			com['thread-pos'] = j+1

			if( len(com['links']) == 0 ):
				continue

			com['provenance'] = {
				'parent': {
					'uri': post['expanded-comments']['uri'],
					'parent': provenance
				}
			}

			extraParams['singleSegIndx'] = genericAddRootCol(
				container['segmented-cols'][colKind], 
				com, 
				container['segmented-cols']['degree-1'][colKind], 
				colSrc + '-comments',
				extraParams=extraParams
			)

		if( containerPrevSize != len(container['segmented-cols'][colKind]) ):
			#count addition of root post and maximum possible elements added to singleSegCol
			#note that tweet without links have been removed
			container['segmented-cols'][colKind][-1]['stats']['total-posts'] = 1 + len( post['expanded-comments']['comments'] )



def redditSegmentCols(src, outputFolder, colSrc, suffix, extraParams=None):

	print('\nredditSegmentCols():')
	if( 'payload' not in src ):
		print('\treturning no payload', src.keys())
		return {}

	if( extraParams is None ):
		extraParams = {}

	cols = {
		'name': colSrc,
		'timestamp': getNowTime(),
		'extraction-timestamp': src['timestamp'],
		'segmented-cols': {
			'ss': [],
			'sm': [],#NA
			'ms': [],
			'mm': [],
			'mc': [],
			'standard-col': 'ss',
			'degree-1': {
				'ss': [],
				'sm': [],#NA
				'ms': [],
				'mm': []
			}
		}
	}

	id = colSrc
	if( suffix != '' ):
		id += '-' + suffix

	if( extraParams['expandDegree1Comments'] ):

		redditSSColAdd(src, cols, colSrc + '-serp', False)
		redditMSColAdd(src, cols, colSrc + '-serp', False)
		redditMMColAdd(src, cols, colSrc + '-serp', False)
	
		redditExpIntDeg1URIs(cols, colSrc)
		doSameForSeg(plts, id, cols, outputFolder, ['sm'], extraParams['generic'])
		src['seg-col-cache'] = cols
		
	elif( 'seg-col-cache' in src ):
		cols = src['seg-col-cache']
		doSameForSeg(plts, id, cols, outputFolder, ['sm'], extraParams['generic'])

	return cols

def twitterSegmentCols(src, outputFolder, colSrc, suffix, extraParams):

	print('\ntwitterSegmentCols():')
	if( 'serp' not in src ):
		print('\treturning, no serp in source')
		return {}

	cols = {
		'name': colSrc,
		'timestamp': getNowTime(),
		"extraction-timestamp": src['serp']['timestamp'],
		'segmented-cols': {
			'ss': [],
			'sm': [],#NA
			'ms': [],
			'mm': [],
			'mc': [],
			'standard-col': 'ss',
			'degree-1': {
				'ss': [],
				'sm': [],#NA
				'ms': [],
				'mm': []
			}
		}
	}

	provenance = {'parent': {'uri': src['serp']['self']}}
	twitterSSColAdd(src['serp'], cols, colSrc, src['degree-1-twt-col'], provenance)
	
	twitterMSColAdd( src['explicit-thread-cols'], cols, colSrc, 'explicit-thread', src['degree-1-twt-col'], provenance )
	twitterMSColAdd( src['implicit-thread-cols'], cols, colSrc, 'implicit-thread', src['degree-1-twt-col'], provenance )
	twitterMMColAdd( src['explicit-thread-cols'], cols, colSrc, 'explicit-thread', src['degree-1-twt-col'], provenance )
	twitterMMColAdd( src['implicit-thread-cols'], cols, colSrc, 'implicit-thread', src['degree-1-twt-col'], provenance )
	
	doSameForSeg(plts, colSrc + '-' + suffix, cols, outputFolder, ['sm'], extraParams['generic'])

	segOutfilename = './Caches/SegmentedCols/' + outputFolder + '/' + colSrc + '.json'
	dumpJsonToFile(segOutfilename, cols, indentFlag=True)
	return cols

def twitterMomentsSegmentCols(src, outputFolder, colSrc, extraParams):

	print('\ntwitterMomentsSegmentCols():')

	if( 'moments' not in src ):
		print('\treturning no payload', src.keys())
		return {}

	if( len(src['moments']) == 0 ):
		print('\treturning empty moments')
		return {}

	cols = {
		'name': colSrc,
		'timestamp': getNowTime(),
		"extraction-timestamp": src['moments'][0]['timestamp'],
		'segmented-cols': {
			'ss': [],#NA
			'sm': [],#NA
			'ms': [],#NA
			'mm': [],
			'mc': [],
			'standard-col': 'ss',
			'degree-1': {
				'ss': [],#NA
				'sm': [],#NA
				'ms': [],#NA
				'mm': []
			}
		}
	}

	
	for msrc in src['moments']:
		provenance = {'parent': {'uri': msrc['self']}}
		twitterMomentsMMColAdd( msrc, cols, colSrc, [msrc['degree-1-twt-col']], provenance )

	doSameForSeg(plts, colSrc, cols, outputFolder, ['ss', 'sm', 'ms'], extraParams['generic'])

	segOutfilename = './Caches/SegmentedCols/' + outputFolder + '/' + colSrc + '.json'
	dumpJsonToFile(segOutfilename, cols, indentFlag=True)

	return cols

def facebookSegmentCols(src, outputFolder, colSrc, extraParams):

	print('\nfacebookSegmentCols():')
	if( 'payload' not in src ):
		print('\treturning no payload', src.keys())
		return {}

	cols = {
		'name': colSrc,
		'timestamp': getNowTime(),
		"extraction-timestamp": src['timestamp'],
		'segmented-cols': {
			'ss': [],
			'sm': [],#NA
			'ms': [],
			'mm': [],
			'mc': [],
			'standard-col': 'ss',
			'degree-1': {
				'ss': [],
				'sm': [],#NA
				'ms': [],
				'mm': []
			}
		}
	}

	provenance = {'parent': {'uri': src['self']}}
	facebookSSColAdd(src, cols, colSrc + '-serp', provenance)
	facebookMSColAdd(src, cols, colSrc + '-serp', provenance)
	facebookMMColAdd(src, cols, colSrc + '-serp', provenance)
	
	doSameForSeg(plts, colSrc, cols, outputFolder, ['sm'], extraParams['generic'])

	segOutfilename = './Caches/SegmentedCols/' + outputFolder + '/' + colSrc + '.json'
	dumpJsonToFile(segOutfilename, cols, indentFlag=True)
		
def sutoriSegmentCols(src, outputFolder, colSrc, extraParams):

	print('\nfacebookSegmentCols():')
	if( 'payload' not in src ):
		print('\treturning no payload', src.keys())
		return {}

	cols = {
		'name': colSrc,
		'timestamp': getNowTime(),
		'extraction-timestamp': src['timestamp'],
		'segmented-cols': {
			'ss': [],
			'sm': [],#NA
			'ms': [],#NA
			'mm': [],#NA
			'mc': [],
			'standard-col': 'ss',
			'degree-1': {
				'ss': [],
				'sm': [],#NA
				'ms': [],#NA
				'mm': []#NA
			}
		}
	}

	sutoriSSColAdd(src, cols, colSrc + '-serp')

	doSameForSeg(plts, colSrc, cols, outputFolder, ['sm', 'ms', 'mm'], extraParams['generic'])

	segOutfilename = './Caches/SegmentedCols/' + outputFolder + '/' + colSrc + '.json'
	dumpJsonToFile(segOutfilename, cols, indentFlag=True)

	return cols

def scoopitSegmentCols(src, outputFolder, colSrc, extraParams):

	print('\nscoopitSegmentCols():')
	if( 'scoops' not in src and 'topics' not in src ):
		print('\treturning no scoops and topics', src.keys())
		return {}

	cols = {
		'name': colSrc,
		'timestamp': getNowTime(),
		"extraction-timestamp": src['scoops']['timestamp'],
		'segmented-cols': {
			'ss': [],
			'sm': [],#NA
			'ms': [],
			'mm': [],#NA
			'mc': [],
			'standard-col': 'ss',
			'degree-1': {
				'ss': [],
				'sm': [],#NA
				'ms': [],
				'mm': []#NA
			}
		}
	}
	
	scoopitSSColAdd(src, cols, colSrc + '-serp')
	scoopitMSColAdd(src, cols, colSrc + '-serp')

	doSameForSeg(plts, colSrc, cols, outputFolder, ['sm', 'mm'], extraParams['generic'])

	segOutfilename = './Caches/SegmentedCols/' + outputFolder + '/' + colSrc + '.json'
	dumpJsonToFile(segOutfilename, cols, indentFlag=True)

	return cols


def addPrereqStats(cols, segs, outputFolder):

	for seg in segs:
		expandURIs( cols['segmented-cols'][seg], outputFolder )
		addMime( cols['segmented-cols'][seg] )
		addMimeDistStats( cols['segmented-cols'][seg] )
		addAge( cols['segmented-cols'][seg], cols['extraction-timestamp'] )

def getSegSignature(seg):

	signature = set()

	for uri in seg['uris']:
		signature.add( uri['post-details']['id'] )

	return signature

def addSSMC(dest, src, median):

	ssToAddToMCX = []
	for i in range(len(src)):

		ssSeg = src[i]
		if( 'text/html' not in ssSeg['stats']['mime-dist'] ):
			continue

		ssSegHtmlLinkCount = ssSeg['stats']['mime-dist']['text/html']
		if( ssSegHtmlLinkCount <= median ):
			continue

		#ssSeg is a potential new member of mcx
		ssSegSign = getSegSignature( ssSeg )
		shouldAddFlag = True

		#check if already member of mcx
		overlap = -1
		for mcxSeg in dest:
			
			if( 'text/html' not in mcxSeg['stats']['mime-dist'] ):
				continue

			if( mcxSeg['stats']['mime-dist']['text/html'] < ssSeg['stats']['mime-dist']['text/html'] ):
				#too small to be parent of ssSeg
				continue

			#mcxSeg is a potential parent of ssSeg
			mcxSegSign = getSegSignature( mcxSeg )
			overlap = overlapFor2Sets(ssSegSign, mcxSegSign)
			if( overlap == 1.0 ):
				shouldAddFlag = False
				break

		if( shouldAddFlag ):
			ssSeg['stats']['ori-frm'] = {'seg': 'ss', 'pos': i}
			ssToAddToMCX.append( ssSeg )

	print('\taddSSMC() will add from ss to mcx:', len(ssToAddToMCX))
	for seg in ssToAddToMCX:
		dest.append( seg )

def addMCAndCountStats(cols, outputFolder, genericConfig, referer):

	basicSegs = ['ss', 'ms', 'mm', 'mc']
	
	cols['segmented-cols']['mc'] = cols['segmented-cols']['ms'] + cols['segmented-cols']['mm']
	#consider adding some ss later
	cols['segmented-cols']['mcx'] = cols['segmented-cols']['ms'] + cols['segmented-cols']['mm']
	addPrereqStats(cols, basicSegs, outputFolder)


	cols.setdefault('stats', {})
	cols['stats'].setdefault('counts', {})
	cols['stats']['overlap'] = {}


	#add stats for all except mcx because mcx needs median stat to add some ss - start
	stats = profSegColAuth.genCounts(cols, basicSegs, referer)
	
	cols['stats']['uri-diversity'] = stats['uri-diversity']	
	for ky, val in stats['counts'].items():
		cols['stats']['counts'][ky] = val
	#add stats for all except mcx because mcx needs median stat to add some ss - end

	#add basic stats for mcx and stats - start
	'''
		medianURICountSS = floor(stats['counts']['html-uri-counts-median-link-count']['ss'])
		addSSMC( cols['segmented-cols']['mcx'], cols['segmented-cols']['ss'], medianURICountSS )
		stats = profSegColAuth.genCounts(cols, ['mcx'], referer)
		
		cols['stats']['uri-diversity']['mcx'] = stats['uri-diversity']['mcx']
		for ky, val in stats['counts'].items():
			cols['stats']['counts'][ky]['mcx'] = val['mcx']
	'''
	#add basic stats for mcx and stats - start

	for vert in ['g', 'nv']:
		serp = getDictFromFile(genericConfig['google-' + vert + '-serp-for-overlap'])
		for seg in ['ss', 'mc', 'mcx']:
			cols['stats']['overlap']['google-' + vert +  '-v-' + seg] = profSegColAuth.measureSerpOverlap(serp, cols['segmented-cols'][seg])


def addStats(cols):

	'''
	if( len(cols['segmented-cols']['ss']) == 0 ):
		return

	if( len(cols['segmented-cols']['ss'][0]['uris']) == 0 ):
		return

	if( 'sim' not in cols['segmented-cols']['ss'][0]['uris'][0] ):
		print('\naddStats(): RETURNING SIM NOT AVAILABLE, RETRY AFTER precEvalSegment.py col')
		return
	'''

	cols.setdefault('stats', {})
	cols['stats'].setdefault('counts', {})

	cols['stats']['author-uri-ranking'] = profSegColAuth.calcAuthorURIRanks( profSegColAuth.getProfileStats(cols) )
	
	stats = profSegColAuth.genRelCounts(cols)
	for ky, val in stats.items():
		cols['stats']['counts'][ky] = val


def genericSegmentCols(cols, outputFolder, colSrc, extraParams=None):
	print('\ngenericSegmentCols')

	if( extraParams is None ):
		extraParams = {}

	doSameForSeg(plts, colSrc, cols, outputFolder, [], extraParams['generic'])

def doSameForSeg(plts, id, cols, outputFolder, naSegs, genericConfig, extraParams={}):

	#max fig count is 15
	#THIS FUNCTION NEEDS REFACTORING, reduce repetition and reduce complexity, and hardcoding
	#THIS FUNCTION NEEDS REFACTORING, reduce repetition and reduce complexity, and hardcoding
	#THIS FUNCTION NEEDS REFACTORING, reduce repetition and reduce complexity, and hardcoding
	#THIS FUNCTION NEEDS REFACTORING, reduce repetition and reduce complexity, and hardcoding
	#THIS FUNCTION NEEDS REFACTORING, reduce repetition and reduce complexity, and hardcoding
	#THIS FUNCTION NEEDS REFACTORING, reduce repetition and reduce complexity, and hardcoding
	print('\ndoSameForSeg():', id)

	addMCAndCountStats(cols, outputFolder, genericConfig, id)
	
	if( genericConfig['plots']['active'] == False ):
		print('\tdrawing OFF' * 2)

	plotOutfolder = './Caches/Plots/' + outputFolder + '/'
	
	goldstandardFile = './PrecEvalRepo/GoldStandards/' + outputFolder +  '/gold.json'
	extraParams.setdefault( 'goldstandard', getDictFromFile(goldstandardFile) )
	extraParams['goldstandard'].setdefault('sim-coeff', -1)

	extraParams.setdefault('outfile', open(plotOutfolder + '/link-dist-dat.txt', 'w'))
	extraParams.setdefault('fig1-link-multibars-dist-cursor', 0)
	'''
		extraParams.setdefault('fig2-link-dist-scatter-plot-cursor', 0)
		extraParams.setdefault('fig4-prec-v-link-count-plot-cursor', 0)
		extraParams.setdefault('fig8-ms-pos-prec-plot-cursor', 0)
		extraParams.setdefault('fig9-mm-pos-prec-plot-cursor', 0)
	'''
	
	extraParams.setdefault('fig3a-html-link-dist-bars-plot-cursor', 0)
	extraParams.setdefault('fig3b-non-html-link-dist-bars-plot-cursor', 0)
	extraParams.setdefault('fig3c-all-link-dist-bars-plot-cursor', 0)

	extraParams.setdefault('fig5a-html-prec-v-link-count-bar-plot-cursor', 0)
	extraParams.setdefault('fig5b-non-html-prec-v-link-count-bar-plot-cursor', 0)
	extraParams.setdefault('fig5c-all-prec-v-link-count-bar-plot-cursor', 0)
	extraParams.setdefault('fig6-age-cdf-plot-cursor', 0)
	extraParams.setdefault('fig7-prec-age-box-plot-cursor', 0)
	extraParams.setdefault('fig10-link-multibars-prec-dist-cursor', 0)
	extraParams.setdefault('fig11-link-multibars-uri-div-dist-cursor', 0)

	boxAgeDiv = genericConfig['plots']['age-box-plot']['divisor']
	ageXLabel = 'Age'
	if( boxAgeDiv == 1 ):
		ageXLabel = 'Age (days)'
	elif( boxAgeDiv == 365 ):
		ageXLabel = 'Age (years)'
	
	linkDistPoints = []
	precAgePoints = []
	precAgeDistForCDFPoints = []
	precAgeBoxCol = []
	posPrecPoints = {
		'ms': [],
		'mm': []
	}
	

	precLinkCountPnts = []
	barsCol = {
		'totalHTMLURICount': [],
		'totalNonHTMLURICount': [],
		'totalAllURICount': []
	}
	precBarsCol = {
		'HTMLURIPrec': [],
		'nonHTMLURIPrec': [],
		'allURIPrec': []
	}

	segScatPltParams = {
		'ageDivisor': boxAgeDiv,
		'simCoeff': extraParams['goldstandard']['sim-coeff'],
		'cutoff': genericConfig['plots']['age-box-plot']['cutoff']
	}
	for seg in ['ss', 'mc', 'ms', 'mm']:

		precLinkCountPnts += addPrecisionStats( id, seg, cols['segmented-cols'][seg], extraParams['goldstandard'], plts['link-dist-scatter-plot'], precBarsCol )
		genericPoints = getSegScatterPlotDat(cols['segmented-cols'][seg], id, seg, barsCol, plts['link-dist-scatter-plot'], params=segScatPltParams)
		
		linkDistPoints += genericPoints['linkDist']
		precAgePoints += genericPoints['precAgeDist']
		
		if( len(genericPoints['precAgeDistForCDF'][0]['points']) != 0 ):
			precAgeDistForCDFPoints += genericPoints['precAgeDistForCDF']
		
		if( len(genericPoints['precAgeBox'][0]['points']) != 0 ):
			precAgeBoxCol += genericPoints['precAgeBox']

		if( seg == 'ms' or seg == 'mm' ):
			posPrecPoints[seg] = genericPoints['posPrecDist'][seg]

		if( genericConfig['plots']['active'] ):
			plotLinkDist(
				cols['segmented-cols'][seg], 
				seg, 
				naSegs,
				id, 
				outputFolder, 
				plts['link-dist-hist'], 
				extraParams
			)
	
	if( genericConfig['plots']['active'] ):
		addStats(cols)

	if( genericConfig['plots']['active'] ):	

		'''
			plt.figure(2)
			genPlotScatter(id, linkDistPoints, plts['link-dist-scatter-plot'], 'fig2-link-dist-scatter-plot-cursor', 'URI Count', 'Post class', extraParams)

			plt.figure(4)
			plt.axhline(extraParams['goldstandard']['sim-coeff'], color='black', linewidth=0.8)
			genPlotScatPltShape(id, precLinkCountPnts, plts['link-dist-scatter-plot'], 'fig4-prec-v-link-count-plot-cursor', 'URI Count', 'Precision', extraParams)

			plt.figure(8)
			plt.axhline(extraParams['goldstandard']['sim-coeff'], color='black', linewidth=0.8)
			genPlotScatPltShape(id, posPrecPoints['ms']['points'], plts['link-dist-scatter-plot'], 'fig8-ms-pos-prec-plot-cursor', 'Thread position', 'Similarity', extraParams)

			plt.figure(9)
			plt.axhline(extraParams['goldstandard']['sim-coeff'], color='black', linewidth=0.8)
			genPlotScatPltShape(id, posPrecPoints['mm']['points'], plts['link-dist-scatter-plot'], 'fig9-mm-pos-prec-plot-cursor', 'Thread position', 'Similarity', extraParams)
		'''
		plt.figure(3)
		plotLinkDistBars(id, barsCol['totalHTMLURICount'], 'Total URIs (HTML)', 'fig3a-html-link-dist-bars-plot-cursor', plts['link-dist-scatter-plot'], naSegs, extraParams)
		plt.figure(10)
		plotLinkDistBars(id, barsCol['totalNonHTMLURICount'], 'Total URIs (non-HTML)', 'fig3b-non-html-link-dist-bars-plot-cursor', plts['link-dist-scatter-plot'], naSegs, extraParams)
		plt.figure(11)
		plotLinkDistBars(id, barsCol['totalAllURICount'], 'Total URIs (All)', 'fig3c-all-link-dist-bars-plot-cursor', plts['link-dist-scatter-plot'], naSegs, extraParams)
		

		plt.figure(5)
		plotPrecVLinkCountBars(id, precBarsCol['HTMLURIPrec'], 'Avg. Prec. (HTML)', 'fig5a-html-prec-v-link-count-bar-plot-cursor', plts['link-dist-scatter-plot'], naSegs, extraParams)
		plt.figure(12)
		plotPrecVLinkCountBars(id, precBarsCol['nonHTMLURIPrec'], 'Avg. Prec. (non-HTML)', 'fig5b-non-html-prec-v-link-count-bar-plot-cursor', plts['link-dist-scatter-plot'], naSegs, extraParams)
		plt.figure(13)
		plotPrecVLinkCountBars(id, precBarsCol['allURIPrec'], 'Avg. Prec. (All)', 'fig5c-all-prec-v-link-count-bar-plot-cursor', plts['link-dist-scatter-plot'], naSegs, extraParams)


		plt.figure(6)
		extraParams['outputFolder'] = outputFolder
		genAgeCDFPlot(id, precAgeDistForCDFPoints, plts['link-dist-scatter-plot'], 'fig6-age-cdf-plot-cursor', ageXLabel, 'Post class', naSegs, extraParams)

		plt.figure(7)
		plotPrecAgeBoxPlot(id, precAgeBoxCol, plts['link-dist-scatter-plot'], 'fig7-prec-age-box-plot-cursor', extraParams)

		plt.figure(15)
		plotDiversity(cols['stats']['uri-diversity'], naSegs, id, plts['link-dist-hist'], 'fig11-link-multibars-uri-div-dist-cursor', extraParams)

		posPrecPath = plotOutfolder + 'PosPrecDist/'
		createFolderAtPath(posPrecPath)
		dumpJsonToFile(posPrecPath + id + '-msmm-pos-prec-dist.json', posPrecPoints)
		
		if( extraParams['fig3a-html-link-dist-bars-plot-cursor'] == plts['link-dist-scatter-plot']['plotStop'] ):
			
			'''
				plt.figure(2)
				fig2.tight_layout()
				plt.savefig(plotOutfolder + 'f2-link-dist-scatt.png')

				plt.figure(4)
				drawPrecVLinkCountLegend( plts['link-dist-scatter-plot'], extraParams )
				fig4.tight_layout()
				plt.savefig(plotOutfolder + 'f4-prec-v-linkcount.png')
				
				plt.figure(8)
				fig8.tight_layout()
				plt.savefig(plotOutfolder + 'f8-ms-pos-prec-dist.png')

				plt.figure(9)
				fig9.tight_layout()
				plt.savefig(plotOutfolder + 'f9-mm-pos-prec-dist.png')
			'''

			plt.figure(3)
			fig3.tight_layout()
			plt.savefig(plotOutfolder + 'f3a-link-dist-bars.png')
			
			plt.figure(10)
			fig10.tight_layout()
			plt.savefig(plotOutfolder + 'f3b-link-dist-bars.png')

			plt.figure(11)
			fig11.tight_layout()
			plt.savefig(plotOutfolder + 'f3c-link-dist-bars.png')
			

			plt.figure(5)
			fig5.tight_layout()
			plt.savefig(plotOutfolder + 'f5a-prec-v-postclass-bars.png')

			plt.figure(12)
			fig12.tight_layout()
			plt.savefig(plotOutfolder + 'f5b-prec-v-postclass-bars.png')

			plt.figure(13)
			fig13.tight_layout()
			plt.savefig(plotOutfolder + 'f5c-prec-v-postclass-bars.png')

			plt.figure(6)
			fig6.tight_layout()
			plt.savefig(plotOutfolder + 'f6-age-dist-cdf.png')
		
			plt.figure(7)
			fig7.tight_layout()
			plt.savefig(plotOutfolder + 'f7-age-dist-box.png')
		
		if( extraParams['fig1-link-multibars-dist-cursor'] == plts['link-dist-hist']['plotStop'] ):
			plt.figure(1)
			fig1.tight_layout()
			plt.savefig(plotOutfolder + 'f1-link-dist-hist.png')

			plt.figure(14)
			fig14.tight_layout()
			plt.savefig(plotOutfolder + 'f10-link-dist-prec-hist.png')

			plt.figure(15)
			fig15.tight_layout()
			plt.savefig(plotOutfolder + 'f11-seg-div.png')

	#review placement
	if( 'fig1-link-multibars-dist-dat' in extraParams ):
		cols['stats']['link-dist'] = extraParams['fig1-link-multibars-dist-dat']
	
	extraParams['outfile'].write(('*' * 80) + '\n')
	extraParams['outfile'].write(('*' * 80) + '\n\n')

def genericGetTitle(pos, id, naSegs):
	naSegs = ', '.join(naSegs)
	naSegs = naSegs.replace('sm, ', '')
	naSegs = naSegs.replace('sm', '')
	if( naSegs != '' ):
		naSegs = ' (NA: ' + naSegs + ')'

	return str(pos) + '. ' + id + naSegs

	return naSegs

def genPlotScatter(id, points, pltDat, cursor, xLabel, yLabel, extraParams):
	
	extraParams[cursor] += 1
	title = str(extraParams[cursor]) + '. ' + id
	
	dataset = {
		'title': title,
		'gridX': pltDat['gridX'],
		'gridY': pltDat['gridY'],
		'xLabel': xLabel,
		'yLabel': yLabel,
		'alpha': 0.5,
		'points': points
	}
	
	plotScatterPlot(plt, dataset, extraParams[cursor])

def genAgeCDFPlot(id, dists, pltDat, cursor, xLabel, yLabel, naSegs, extraParams):
	
	extraParams[cursor] += 1

	dataset = {
		'title': genericGetTitle(extraParams[cursor], id, naSegs),
		'gridX': pltDat['gridX'],
		'gridY': pltDat['gridY'],
		'xLabel': xLabel,
		'yLabel': yLabel,
		'dists': dists
	}
	
	pltPoints = plotCDF2(plt, dataset, extraParams[cursor])
	dumpJsonToFile('./Caches/CDFs/' + extraParams['outputFolder'] + '/' + id + '.json', pltPoints)

def plotDiversity(divs, naSegs, id, pltDat, cursor, extraParams):

	for seg in ['ss', 'mc', 'ms', 'mm']:
		extraParams[cursor] += 1
		title = str(extraParams[cursor]) + '. ' + id
		multiBarsCol = []
		
		if( 'uri-diversity' in divs[seg] and 'hostname-diversity' in divs[seg] ):
			multiBarsCol = [
				{
					'name': 'URI',
					'bars': [{
						'height': round(divs[seg]['uri-diversity'], 2),
						'color': pltDat['colormap'][seg]
					}]
				},
				{
					'name': 'Hostname',
					'bars': [{
						'height': round(divs[seg]['hostname-diversity'], 2),
						'color': pltDat['colormap'][seg]
					}]
				}
			]

		dataset = {
			'title': title,
			'yLabel': 'Diversity',
			'xLabel': 'Policy',
			'gridX': pltDat['gridX'],
			'gridY': pltDat['gridY'],
			'yLim': [0, 1],
			'barWidth': 0.6,
			'barsCol': multiBarsCol,
		}

		if( seg in naSegs ):
			plt.subplot(pltDat['gridX'], pltDat['gridY'], extraParams[cursor])
			plt.text(0.5, 0.5, 'NA', dict(size=30), horizontalalignment='center', verticalalignment='center')
			plt.title(title)

			frame1 = plt.gca()
			frame1.axes.get_xaxis().set_visible(False)
			frame1.axes.get_yaxis().set_visible(False)
		else:
			plotMultiBarPlot(
				plt, 
				dataset,
				extraParams[cursor]
			)

def plotPrecAgeBoxPlot(id, collection, pltDat, cursor, extraParams):
	extraParams[cursor] += 1
	title = str(extraParams[cursor]) + '. ' + id

	dataset = {
		'title': title,
		'gridX': pltDat['gridX'],
		'gridY': pltDat['gridY'],
		'xLabel': 'Post class',
		'yLabel': 'Age (years)',
		'collection': collection
	}

	plotBoxPlot(plt, dataset, extraParams[cursor])

def plotLinkDistBars(id, barsCol, yLabel, cursor, pltDat, naSegs, extraParams):

	extraParams[cursor] += 1
	title = str(extraParams[cursor]) + '. ' + id
	
	naSegs = ', '.join(naSegs)
	naSegs = naSegs.replace('sm, ', '')
	naSegs = naSegs.replace('sm', '')
	if( naSegs != '' ):
		naSegs = ' (NA: ' + naSegs + ')'

	mcTotal = 0
	modBarCol = []
	msmmBars = []
	for bar in barsCol:
		if( bar['name'] == 'mc' ):
			continue
		elif( bar['name'] == 'ss' ):
			modBarCol.append( bar )
		else:
			mcTotal += bar['bars'][0]['height']
			msmmBars.append( bar['bars'][0] )

	modBarCol.append({
		'name': 'mc (' + str(mcTotal) + ')',
		'bars': msmmBars
	})

	dataset = {
		'title': title + naSegs,
		'yLabel': yLabel,
		'xLabel': 'Post class',
		'gridX': pltDat['gridX'],
		'gridY': pltDat['gridY'],
		'barsCol': modBarCol,
	}

	'''
	plotMultiBarPlot(
		plt, 
		dataset,
		extraParams[cursor]
	)
	'''

	plotStackedBarPlot(
		plt, 
		dataset,
		extraParams[cursor]
	)

def plotPrecVLinkCountBars(id, precBarsCol, yLabel, cursor, pltDat, naSegs, extraParams):

	extraParams[cursor] += 1
	title = str(extraParams[cursor]) + '. ' + id

	naSegs = ', '.join(naSegs)
	naSegs = naSegs.replace('sm, ', '')
	naSegs = naSegs.replace('sm', '')
	if( naSegs != '' ):
		naSegs = ' (NA: ' + naSegs + ')'

	dataset = {
		'title': title + naSegs,
		'yLabel': yLabel,
		'xLabel': 'Post class',
		'gridX': pltDat['gridX'],
		'gridY': pltDat['gridY'],
		'yLim': [0, 1],
		'barWidth': 0.6,
		'barsCol': precBarsCol
	}

	plotMultiBarPlot(
		plt, 
		dataset,
		extraParams[cursor]
	)

	#fix later, due to approx to 1dp, sim coeff is not 0.3, but 0.25, since when 0.25 is approx to 1dp it becomes 0.3
	plt.axhline(0.25, color='black', linewidth=0.8)

def drawPrecVLinkCountLegend(pltDat, extraParams):

	
	handles = []

	for seg, shape in pltDat['shapemap'].items():
		hand = mlines.Line2D([], [], color=pltDat['colormap'][seg], marker=shape, linestyle='None', markersize=10, label=seg)
		handles.append( hand )

	plt.legend(handles=handles, loc='center')


	'''
		extraParams['fig4-prec-v-link-count-plot-cursor'] += 1
		handles = []

		for seg, shape in pltDat['shapemap'].items():
			hand = mlines.Line2D([], [], color=pltDat['colormap'][seg], marker=shape, linestyle='None', markersize=10, label=seg)
			handles.append( hand )
		
		plt.subplot(pltDat['gridX'], pltDat['gridY'], extraParams['fig4-prec-v-link-count-plot-cursor'])
		plt.text(0.25, 0.8, 'Legend', dict(size=30))
		plt.legend(handles=handles, loc='center')

		frame1 = plt.gca()
		frame1.axes.get_xaxis().set_visible(False)
		frame1.axes.get_yaxis().set_visible(False)
	'''

def genPlotScatPltShape(id, points, pltDat, cursor, xLabel, yLabel, extraParams):

	extraParams[cursor] += 1
	title = str(extraParams[cursor]) + '. ' + id

	dataset = {
		'title': title,
		'gridX': pltDat['gridX'],
		'gridY': pltDat['gridY'],
		'xLabel': xLabel,
		'yLabel': yLabel,
		'points': points
	}

	plotScatterPlotShape(plt, dataset, extraParams[cursor])

def plotLinkDist(segments, seg, naSegs, id, outputFolder, pltDat, extraParams):

	figs = {
		'freqDistBarsCol': {
			'cursor': 'fig1-link-multibars-dist-cursor',
			'yLabel': 'Post Freq.',
			'figNo': 1,
		},
		'precLinkDistBarsCol': {
			'cursor': 'fig10-link-multibars-prec-dist-cursor',
			'yLabel': 'Avg. Prec.',
			'figNo': 14,
			'yLim': [0, 1]
		}
	}

	extraParams.setdefault('fig1-link-multibars-dist-dat', {})
	extraParams['fig1-link-multibars-dist-dat'][seg] = {}
	extraParams['fig1-link-multibars-dist-dat'][seg]['payload'] = []

	for plot, params in figs.items():
		
		plt.figure(params['figNo'])
		cursor = params['cursor']
		yLabel = params['yLabel']
		
		multiBarsCol = getLinkDistPlotDat( 
			segments, 
			seg, 
			id, 
			outputFolder, 
			pltDat['colormap'][seg]
		 )

		#write link dist dat - start
		if( plot == 'freqDistBarsCol' ):
			for i in range( len(multiBarsCol[plot]) ):
				extraParams['fig1-link-multibars-dist-dat'][seg]['payload'].append({
					'bin': multiBarsCol[plot][i]['name'],
					'freq': multiBarsCol[plot][i]['bars'][0]['height'],
					'rel-count': multiBarsCol[plot][i]['bars'][0]['rel-count']
				})
				
		#write link dist dat - end
		
		extraParams[cursor] += 1
		title = str(extraParams[cursor]) + '. ' + id + ': ' + seg
		dataset = {
			'title': title,
			'yLabel': yLabel,
			'xLabel': 'URI Count Bins',
			'gridX': pltDat['gridX'],
			'gridY': pltDat['gridY'],
			'barWidth': 0.6,
			'barsCol': multiBarsCol[plot],
		}

		if( 'yLim' in params ):
			dataset['yLim'] = params['yLim']

		if( seg in naSegs ):
			plt.subplot(pltDat['gridX'], pltDat['gridY'], extraParams[cursor])
			plt.text(0.5, 0.5, 'NA', dict(size=30), horizontalalignment='center', verticalalignment='center')
			plt.title(title)

			frame1 = plt.gca()
			frame1.axes.get_xaxis().set_visible(False)
			frame1.axes.get_yaxis().set_visible(False)
		else:
			plotMultiBarPlot(
				plt, 
				dataset,
				extraParams[cursor]
			)

def getLongURI(shorturi, cacheFolder):

	shorturi = shorturi.strip()
	if( len(shorturi) == 0 ):
		return ''

	uriHash = getURIHash( shorturi )
	mappingFilename = './Caches/ShortURIs/' + cacheFolder + '/' + uriHash + '.json'
	res = getDictFromFile( mappingFilename )

	if( 'long-uri' in res and 'short-uri' in res ):
		if( isSameLink(shorturi, res['short-uri']) ):
			return res['long-uri']

	return ''

def cacheShortURIMap(shorturi, longuri, cacheFolder):
	
	shorturi = shorturi.strip()
	longuri = longuri.strip()

	if( len(shorturi) == 0 or len(longuri) == 0 ):
		return

	uriHash = getURIHash( shorturi )
	mappingFilename = './Caches/ShortURIs/' + cacheFolder + '/' + uriHash + '.json'
	
	payload = {
		'short-uri': shorturi,
		'long-uri': longuri
	}
	
	dumpJsonToFile(mappingFilename, payload)

def expandURIs(segments, cacheFolder):

	jobLst = []
	grandTotal = len(segments)
	for i in range(grandTotal):

		counter = 0
		total = len(segments[i]['uris'])
		for j in range(total):
			
			if( segments[i]['uris'][j]['custom']['is-short'] == False ):
				continue

			#skip if long-uri already available
			if( 'long-uri' in segments[i]['uris'][j]['custom'] ):
				if( segments[i]['uris'][j]['custom']['long-uri'] != '' ):
					continue

			longURI = getLongURI( segments[i]['uris'][j]['uri'], cacheFolder )
			if( longURI != '' ):
				segments[i]['uris'][j]['custom']['long-uri'] = longURI
				continue

			keywords = {
				'url': segments[i]['uris'][j]['uri']
			}

			toPrint = ''
			if( counter % 10 == 0 ):
				toPrint = '\t' + str(counter) + ' of ' + str(total) + ', ' + str(i) + ' of ' + str(grandTotal)

			jobLst.append( {'func': expandUrl, 'args': keywords, 'misc': {'i': i, 'j': j}, 'print': toPrint} )
			counter += 1


	if( len(jobLst) == 0 ):
		return
	
	print('\nexpandURIs():')
	resLst = parallelTask(jobLst)
	for rs in resLst:
		indx = rs['misc']
		i = indx['i']
		j = indx['j']
		segments[i]['uris'][j]['custom']['long-uri'] = rs['output']

		if( len(rs['output']) != 0 ):
			cacheShortURIMap( rs['input']['args']['url'], rs['output'], cacheFolder )

def addMime(segments):

	for i in range(len(segments)):
		for j in range(len(segments[i]['uris'])):
			
			#skip if mime already available
			if( 'mime' in segments[i]['uris'][j]['custom'] ):
				if( segments[i]['uris'][j]['custom']['mime'] != '' ):
					continue

			uri = segments[i]['uris'][j]['uri']

			if( segments[i]['uris'][j]['custom']['is-short'] ):
				if( 'long-uri' in segments[i]['uris'][j]['custom'] ):
					if( segments[i]['uris'][j]['custom']['long-uri'] != '' ):

						uri = segments[i]['uris'][j]['custom']['long-uri']

			segments[i]['uris'][j]['custom']['mime'] = genericGetMimeType(uri)

def addAge(segments, captureDate):

	captureDate = datetime.strptime( captureDate, '%Y-%m-%d %H:%M:%S' )

	for i in range(len(segments)):
		for j in range(len(segments[i]['uris'])):
			uri = segments[i]['uris'][j]

			if( 'hash' in uri ):
				uriHash = uri['hash']
			else:
				uriHash = getURIHash( uri['uri'] )

			uri['age-days'] = -1
			uri['pub-date'] = ''

			cdFile = './Caches/CD/' + uriHash + '.txt'
			if( os.path.exists(cdFile) == False ):
				continue

			pubDate = readTextFromFile(cdFile)
			pubDate = pubDate.strip().split('.')[0]
			
			if( pubDate != '' ):
				try:

					if( uri['post-details']['creation-date'].strip() == '' ):
						postPubdate = datetime.now()
					else:
						postPubdate = datetime.strptime(uri['post-details']['creation-date'], '%Y-%m-%d %H:%M:%S')

					cdPubdate = datetime.strptime(pubDate, '%Y-%m-%d %H:%M:%S')
					if( cdPubdate < postPubdate ):
						uri['pub-date'] = pubDate
						pubDate = cdPubdate
					else:
						uri['pub-date'] = uri['post-details']['creation-date']
						uri['custom']['using-postdate-not-cd'] = pubDate
						pubDate = postPubdate

					
					uri['age-days'] = (captureDate - pubDate).days
				except:
					genericErrorInfo()
					print('\tpost-pubDate:', uri['post-details']['creation-date'])
					print('\tpubDate:', pubDate)

def addMimeDistStats(segments):

	for i in range(len(segments)):

		segments[i]['stats']['mime-dist'] = {}
		for j in range(len(segments[i]['uris'])):

			mime = segments[i]['uris'][j]['custom']['mime']
			segments[i]['stats']['mime-dist'].setdefault(mime, 0)
			segments[i]['stats']['mime-dist'][mime] += 1

def addPrecisionStats(id, seg, segments, goldstandard, pltDat, precBarsCol):

	print('\naddPrecisionStats()', id, seg)
	segSize = len(segments)
	
	#check if these segments have already been processed through precEvalSegment.py
	for i in range(segSize):
		for j in range(len(segments[i]['uris'])):

			uri = segments[i]['uris'][j]
			uriHash = getURIHash( uri['uri'] )

			cosineSimFile = './Caches/CosineSim/' + uriHash + '.json'
			if( os.path.exists(cosineSimFile) == False ):
				print('\t\tCOSINE SIM FILE MISSING, returning:', seg, 'RUN precEvalSegment for:', id)
				return []

	precLinkCountPnts = []
	avgPrec = {
		'HTMLURIPrec': {'avgPrec': 0, 'total': 0},
		'nonHTMLURIPrec': {'avgPrec': 0, 'total': 0},
		'allURIPrec': {'avgPrec': 0, 'total': 0}
	}

	for i in range(segSize):
		
		printSuffix = '\tseg: ' + id + ', ' + seg + ' ' + str(i) + ' of ' + str(segSize)
		precEvalCol(goldstandard, testCol=segments[i], printSuffix=printSuffix)
		
		if( segments[i]['predicted-precision'] == -1 ):
			continue
		
		mimes = list( segments[i]['stats']['mime-dist'].keys() )
		if( len(mimes) == 0 ):
			continue

		pnt = {
			'x': -1,
			'y': segments[i]['predicted-precision'],
			'color': pltDat['colormap'][seg],
			'shape': pltDat['shapemap'][seg],
			'alpha': 0.5
		}

		avgPrec['allURIPrec']['total'] += 1
		avgPrec['allURIPrec']['avgPrec'] += pnt['y']

		if( 'text/html' in mimes ):
			
			mimes.remove('text/html')
			uriCount = segments[i]['stats']['mime-dist']['text/html']
			pnt['x'] = uriCount

			avgPrec['HTMLURIPrec']['total'] += 1
			avgPrec['HTMLURIPrec']['avgPrec'] += pnt['y']
			precLinkCountPnts.append( pnt )
		
		if( len(mimes) != 0 ):
			#here means text/html coexists with atleast 1 more mime type
			avgPrec['nonHTMLURIPrec']['total'] += 1
			avgPrec['nonHTMLURIPrec']['avgPrec'] += pnt['y']
		


	for col, colList in precBarsCol.items():
		
		if( avgPrec[col]['total'] == 0 ):
			avgPrec[col]['avgPrec'] = 0
		else:
			avgPrec[col]['avgPrec'] = round( avgPrec[col]['avgPrec'] / avgPrec[col]['total'], 2 )

		datPoint = {
				'name': seg,
				'bars': [{
					'height': avgPrec[col]['avgPrec'],
					'color': pltDat['colormap'][seg],
					'text': str(avgPrec[col]['avgPrec']) + '\n(' + str(avgPrec[col]['total']) + ')'
				}]
			}
		colList.append( datPoint )


	return precLinkCountPnts

def isRel(sim, goldSim):
	#use PrecEval.py's version
	sim = round(sim, 1)
	goldSim = round(goldSim, 1)

	if( sim >= goldSim ):
		return True
	else:
		return False

			
def getLinkDistPlotDat(segments, seg, id, outputFolder, color):
	
	#print('\ngetLinkDistPlotDat():', seg, id)

	freqDist = {
		'1': {'postCount': 0, 'relCount': 0},#how many posts have 1 html uri
		'2': {'postCount': 0, 'relCount': 0},
		'3-4': {'postCount': 0, 'relCount': 0},
		'5+': {'postCount': 0, 'relCount': 0}
	}

	precLinkDist = {
		'1': {'avgPrec': 0},# for seg posts with 1 uri
		'2': {'avgPrec': 0},# for seg posts with 2 uris
		'3-4': {'avgPrec': 0},
		'5+': {'avgPrec': 0}
	}
	
	freqDistBarsCol = []
	precLinkDistBarsCol = []
	for i in range(len(segments)):
		
		if( 'text/html' not in segments[i]['stats']['mime-dist'] ):
			continue
		freq = segments[i]['stats']['mime-dist']['text/html']
		isRelevant = isRel( segments[i]['predicted-precision'], segments[i]['sim-coeff'] )
		
		if( isRelevant ):
			isRelevant = 1
		else:
			isRelevant = 0
		
		if( freq == 1 ):
			
			freqDist['1']['postCount'] += 1
			freqDist['1']['relCount'] += isRelevant
			precLinkDist['1']['avgPrec'] += segments[i]['predicted-precision']
		
		elif( freq == 2 ):
			
			freqDist['2']['postCount'] += 1
			freqDist['2']['relCount'] += isRelevant
			precLinkDist['2']['avgPrec'] += segments[i]['predicted-precision']
		
		elif( freq > 2 and freq < 5 ):
			
			freqDist['3-4']['postCount'] += 1
			freqDist['3-4']['relCount'] += isRelevant
			precLinkDist['3-4']['avgPrec'] += segments[i]['predicted-precision']
		
		elif( freq > 4 ):
			
			freqDist['5+']['postCount'] += 1
			freqDist['5+']['relCount'] += isRelevant
			precLinkDist['5+']['avgPrec'] += segments[i]['predicted-precision']

	

	for bin, countDct in freqDist.items():
		#print('\t', bin, count)
		
		if( countDct['postCount'] == 0 ):
			precLinkDist[bin] = 0
		else:
			precLinkDist[bin] = round( precLinkDist[bin]['avgPrec'] / countDct['postCount'], 2 )
		
		freqDistBarsCol.append({
			'name': bin,
			'bars': [{
				'height': countDct['postCount'],
				'rel-count': countDct['relCount'],
				'color': color
			}]
		})

		if( precLinkDist[bin] < 0 ):
			precLinkDist[bin] = 0

		precLinkDistBarsCol.append({
			'name': bin,
			'bars': [{
				'height': precLinkDist[bin],
				'color': color
			}]
		})

	return {
		'freqDistBarsCol': freqDistBarsCol,
		'precLinkDistBarsCol': precLinkDistBarsCol
	}

def getSegScatterPlotDat(segments, id, seg, barsCol, pltDat, params):
	
	params.setdefault('ageDivisor', 1)
	#verified: linkDist
	pointsDict = {
		'linkDist': [],
		'precAgeDist': [],
		'precAgeDistForCDF': [{
			'label': seg,
			'color': pltDat['colormap'][seg],
			'linestyle': pltDat['linemap'][seg],
			'points': []
		}],
		'precAgeBox': [{
			'name': seg,
			'points': []
		}],
		'posPrecDist': {
			'ms': {
				'prob': {},
				'points': []
			},
			'mm': {
				'prob': {},
				'points': []
			}
		}
	}
	
	uriDist = {
		'totalHTMLURICount': 0,
		'totalAllURICount': 0,
		'totalNonHTMLURICount': 0
	}
	
	for i in range(len(segments)):
		for uri in segments[i]['uris']:

			if( 'sim' in uri ):

				if( (seg == 'ms' or seg == 'mm') and uri['sim'] != -1 and 'thread-pos' in uri['post-details'] ):
					xVal = uri['post-details']['thread-pos']
					pointsDict['posPrecDist'][seg]['points'].append({
						'x': xVal,
						'y': uri['sim'],
						'color': pltDat['colormap'][seg],
						'shape': pltDat['shapemap'][seg],
						'alpha': 0.5,
						'uri': uri['uri'],
						'post': uri['post-details']['uri']
					})

					if( 'relevant' in uri ):
						
						pointsDict['posPrecDist'][seg]['prob'].setdefault(xVal, {'rel': 0, 'total': 0})
						pointsDict['posPrecDist'][seg]['prob'][xVal]['total'] += 1
						
						if( uri['relevant'] ):
							pointsDict['posPrecDist'][seg]['prob'][xVal]['rel'] += 1
					


			#CAUTION, NOTE CONTINUE STATEMENT
			#CAUTION, NOTE CONTINUE STATEMENT
			#CAUTION, NOTE CONTINUE STATEMENT
			#CAUTION, NOTE CONTINUE STATEMENT
			if( 'relevant' not in uri ):
				continue

			if( uri['relevant'] and uri['age-days'] > -1 ):

				xVal = uri['age-days']/params['ageDivisor']
				if( xVal < params['cutoff'] ):
					pointsDict['precAgeDist'].append({
						'x': xVal,
						'y': seg,
						'color': pltDat['colormap'][seg],
						'alpha': 0.5
					})
					pointsDict['precAgeBox'][0]['points'].append( xVal )
					pointsDict['precAgeDistForCDF'][0]['points'].append( xVal )

		for mime, freq in segments[i]['stats']['mime-dist'].items():
			uriDist['totalAllURICount'] += freq

			if( mime == 'text/html' ):
				
				uriCount = segments[i]['stats']['mime-dist']['text/html']
				uriDist['totalHTMLURICount'] += uriCount

				pointsDict['linkDist'].append({
					'x': uriCount,
					'y': seg,
					'color': pltDat['colormap'][seg],
					'alpha': 0.5
				})
			else:
				uriDist['totalNonHTMLURICount'] += freq

	#outside for
	for col, colList in barsCol.items():
		datPoint = {
				'name': seg,
				'bars': [{
					'height': uriDist[col],
					'color': pltDat['colormap'][seg],
					'text': seg + ': ' + str(uriDist[col])
				}]
			}
		colList.append( datPoint )

	#add prob of rel
	for seg, segDct in pointsDict['posPrecDist'].items():
		for threadPos, threadPosDct in segDct['prob'].items():
			threadPosDct['rel-prob'] = threadPosDct['rel']/threadPosDct['total']
			'''
			pointsDict['posPrecDist'][seg]['points'].append({
					'x': threadPos,
					'y': threadPosDct['rel-prob'],
					'color': 'black',
					'shape': '.',
					'alpha': 1
				})
			'''
			

	return pointsDict


#reddit deg 1 uri exploration - start
def redditExpIntDeg1URIs(segCols, colSrc):
	if( len(segCols) == 0 ):
		return segCols

	print('\nredditExpIntDeg1URIs():')
	extraParamsCols = {}
	markedForRemoval = 0
	uriMappingDct = {}
	urlList = []

	#collect degree 1 comments to process - start
	#debugBreakFlag = False
	for colKind in ['ss', 'sm', 'ms', 'mm']:

		extraParamsCols[colKind] = {'extraParams': {'dedupSet': set(), 'degree': 2} }
		for i in range( len(segCols['segmented-cols'][colKind]) ):

			segCol = segCols['segmented-cols'][colKind][i]
			segColURILength = len(segCol['uris'])
			for j in range(segColURILength):

				uriDct = segCol['uris'][j]
				uri = uriDct['uri']

				intDeg1Flag = uriDct['custom']['uri-type'].find('internal-degree')
				shortlinkFlag = uriDct['custom']['uri-type'].find('shortlink')
				
				if( intDeg1Flag == -1 and shortlinkFlag == -1 ):
					extraParamsCols[colKind]['extraParams']['dedupSet'].add( getDedupKeyForURI(uri) )
					continue		

				uri = uri.split('://')[-1].replace('www.reddit.com/', '')
				if( uri.find('/comments/') == -1 and shortlinkFlag == -1 ):
					uriDct['custom']['remove'] = True
					markedForRemoval += 1
					continue
				
				if( shortlinkFlag == -1 ):
					shortlinkFlag = ''
				else:
					shortlinkFlag = expandUrl( uriDct['uri'] )
					uriDct['long-uri'] = shortlinkFlag

					print('\t', j, 'of', segColURILength, 'short:', uri)
					print('\t\tlong uri:', shortlinkFlag)

				uriDct['custom']['remove'] = True
				uriMappingDct[ len(urlList) ] = {
					'uri': uriDct['uri'], 
					'colKind': colKind, 
					'longLink': shortlinkFlag, 
					'segColPos': i,
					'uriPos': j,
					'postUri': uriDct['post-details']['uri'],
					'postUriProvenance': uriDct['post-details']['provenance']
				}

				if( shortlinkFlag == '' ):
					urlList.append( uriDct['uri'] )
				else:
					urlList.append( shortlinkFlag )

				'''
				if( len(urlList) == 10 ):
					debugBreakFlag = True
					break
			
			if( debugBreakFlag ):
				break

		if( debugBreakFlag ):
			break
		'''
			
	#collect degree 1 comments to process - end

	#visit degree 1 comments
	genRedditExtraParams = {'addRootComment': True, 'maxSleep': 2, 'excludeCommentsWithNoLinks': False}
	res = redditPrlGetLinksFromComment(urlList, extraParams=genRedditExtraParams)
	segCols['degree-1-dets'] = {
		'count-to-remove': markedForRemoval,
		'count-to-process': len(urlList),
		'links-to-process': urlList
	}

	redditAddReplacement(segCols, res, uriMappingDct, colSrc, extraParamsCols)
	redditFinalizeSegCols(segCols)

def redditRmIntDegLinks(singleSeg):
	
	newSingleSeg = []
	for uriDct in singleSeg:
		if( uriDct['custom']['uri-type'].find('internal-degree') == -1 ):
			uriDct['custom']['degree'] = 2
			newSingleSeg.append(uriDct)

	return newSingleSeg

def redditAddReplacement(segCols, extPost, uriMappingDct, colSrc, extraParamsCols):
	
	'''
	after the exploration of a degree 1 links check if a degree 1 link can be replaced 
	with external links the note add replacement data where degree 1 link resides
	'''
	for i in range(len(extPost)):
		
		if( len(extPost[i]) == 0 ):
			continue

		if( 'comments' not in extPost[i] ):
			continue

		if( len(extPost[i]['comments']) == 0 ):
			continue


		uriMap = uriMappingDct[i]
		shortURLFlag = ''
		if( len(uriMap['longLink']) == 0 ):
			inputURI = uriMap['uri']
		else:
			inputURI = uriMap['longLink']
			shortURLFlag = uriMap['uri']


		if( inputURI != extPost[i]['input-uri'] ):
			print('\tINPUT-OUTPUT URI MISMATCH, NOT PROCEEDING' * 100)
			continue

		singleSeg = []
		colKind = uriMap['colKind']
		post = extPost[i]['comments'][0]#0: root post, subsequent indices: comments, it was the root post that was the embeded degree-1 link so only pay attention to it an not its descendants
		post['thread-pos'] = 0
		post['provenance'] = {
			'parent': {
				'uri': uriMap['postUri'],
				'parent': uriMap['postUriProvenance']
			}
		}

		if( shortURLFlag != '' ):
			post['custom']['custom'] = {'short-uri': shortURLFlag}
		
		redditAddRootCol(
			singleSeg, 
			post,
			colSrc + '-serp',
			extraParams=extraParamsCols[colKind]['extraParams']
		)

		if( len(singleSeg) != 0 ):
			
			singleSeg = singleSeg[0]
			singleSeg['uris'] = redditRmIntDegLinks( singleSeg['uris'] )

			segColPos = uriMap['segColPos']
			uriPos = uriMap['uriPos']
			
			if( len(singleSeg['uris']) != 0 ):
				#all these uris are from a single post
				segCols['segmented-cols'][colKind][segColPos]['uris'][uriPos]['custom']['replacement'] = singleSeg['uris']

def redditFinalizeSegCols(segCols):

	'''
	remove links marked for removal
	if a link marked for removal has replacement use replacement in place of link
	'''

	for colKind in ['ss', 'sm', 'ms', 'mm']:
		
		newSegCol = []
		for i in range( len(segCols['segmented-cols'][colKind]) ):

			deg2URICount = 0
			newSegColURIs = []
			segCol = segCols['segmented-cols'][colKind][i]
			segColURILength = len(segCol['uris'])
			
			for j in range(segColURILength):

				uriDct = segCol['uris'][j]
				if( 'remove' in uriDct['custom'] ):

					if( 'replacement' in uriDct['custom'] ):
						deg2URICount += len(uriDct['custom']['replacement'])
						newSegColURIs += uriDct['custom']['replacement']
						uriDct['custom']['replaced-by-count'] = len(uriDct['custom']['replacement'])

					#move this to deg col in segCol
					segCols['segmented-cols']['degree-1'][colKind].append(uriDct)
				else:
					#not marked for removal
					newSegColURIs.append(uriDct)
			
			segCols['segmented-cols'][colKind][i]['uris'] = newSegColURIs
			if( len(newSegColURIs) != 0 ):
				newSegCol.append( segCols['segmented-cols'][colKind][i] )

			if( deg2URICount != 0 ):
				segCols['segmented-cols'][colKind][i]['stats']['degree-2-links-count'] = deg2URICount
				segCols['segmented-cols'][colKind][i]['stats']['uri-count'] = len(newSegColURIs)

		segCols['segmented-cols'][colKind] = newSegCol

#reddit deg 1 uri exploration - end