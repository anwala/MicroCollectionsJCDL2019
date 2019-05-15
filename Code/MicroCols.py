import sys

from datetime import datetime
from genericCommon import getNowTime
from genericCommon import getDomain
from genericCommon import getTweetLink
from genericCommon import dumpJsonToFile
from genericCommon import getDictFromFile
from genericCommon import createFolderAtPath
from genericCommon import redditSearchExpand
from genericCommon import parseTweetURI
from genericCommon import twitterGetTweetFromMoment
from genericCommon import wikipediaGetExternalLinksDictFromPage

from genericCommon import scoopitExtractTopics
from genericCommon import scoopitExtractPosts
from genericCommon import scoopitSearch
from genericCommon import sutoriSearch
from genericCommon import genericErrorInfo
from genericCommon import expandUrl

import SegmentCols
import socMedGenCol

class ExtractMicroCol(object):

	def __init__(self, configFilename):		

		print('\nExtractMicroCol::init() - start')
		reportFilename = configFilename.replace('config.json', 'report.json')
		
		print('\tconfig:', configFilename)
		print('\treport:', reportFilename)
		

		self.cols = getDictFromFile(configFilename)
		self.cache = getDictFromFile(reportFilename)
		self.reportFilename = reportFilename
		self.health = False

		if( 'sources' in self.cols and 'collectionTopic' in self.cols ):

			createFolderAtPath('./Caches/Deg1Twttr/' + self.cols['collectionTopic'])
			createFolderAtPath('./Caches/ExpTwttrThreads/' + self.cols['collectionTopic'])
			createFolderAtPath('./Caches/ImpTwttrThreads/' + self.cols['collectionTopic'])
			createFolderAtPath('./Caches/Sources/' + self.cols['collectionTopic'])
			createFolderAtPath('./Caches/Tweets/' + self.cols['collectionTopic'])
			createFolderAtPath('./Caches/SegmentedCols/' + self.cols['collectionTopic'])
			createFolderAtPath('./Caches/ShortURIs/' + self.cols['collectionTopic'])
			createFolderAtPath('./Caches/Plots/' + self.cols['collectionTopic'])
			createFolderAtPath('./Caches/CDFs/' + self.cols['collectionTopic'])
			
			print('\tsources:', len(self.cols['sources']))
			for src in self.cols['sources']:
				print('\t\t', src['name'], 'active:', src['active'])
				self.cols

			self.extractCols()
			self.health = True
		else:
			print('\tmissing config')


		print('ExtractMicroCol::init() - end\n')

	def writeReport(self, indentFlag=False):
		if( self.health ):
			dumpJsonToFile(self.reportFilename, self.cols, indentFlag=indentFlag)

	def getColFromCache(self, colname, id):

		if( len(self.cache) == 0 ):
			return {}

		singleSrcFilename = './Caches/Sources/' + self.cols['collectionTopic'] + '/' + id + '.json'
		src = getDictFromFile(singleSrcFilename)
		if( len(src) != 0 ):
			print('\tgetColFromCache():', id, 'HIT 1')
			return src

		for src in self.cache['sources']:
			if( src['name'] == colname and src['id'] == id ):
				if( 'output' in src ):
					if( len(src['output']) != 0 ):
						print('\tgetColFromCache():', src['name'], 'HIT 2')
						return src['output']

		
		

		return {}

	@staticmethod
	def addDegree1TwtLinks(twts, container, dedupSet):

		for twt in twts:
			for l in twt['tweet-links']:
				
				twtFromLink = parseTweetURI(l)
				if( twtFromLink['id'] != '' and twtFromLink['id'] not in dedupSet ):
					
					container.append({ 'uri': l, 'parent': getTweetLink(twt['data-screen-name'], twt['data-tweet-id']) })
					dedupSet.add( twtFromLink['id'] )

	def genTwitterCols(self, name, settings):
		print('\ngenTwitterCols():')

		output = {
			'serp': {}, 
			'explicit-thread-cols': {},
			'implicit-thread-cols': {},
			'serp-heuristics': {}
		}

		filename = settings['inputFileWithTweets'].split('/')[-1].replace('.json', '')
		twtCol = getDictFromFile( settings['inputFileWithTweets'] )
		
		twtCol = socMedGenCol.normalizeCol(twtCol)
		output['serp'] = twtCol

		threadOptions = {
			'extractExpThreadCol': {
				'input': 'explicit-thread-links', 
				'output': 'explicit-thread-cols', 
				'trim': 'maxExpThreadToExplore',
				'expFlag': True
			},
			'extractImpThreadCol': {
				'input': 'possible-implicit-thread-links', 
				'output': 'implicit-thread-cols', 
				'trim': 'maxImpThreadToExplore', 
				'expFlag': False
			}
		}

		for threadOption, params in threadOptions.items():
			if( settings[threadOption] ):
				
				trim = settings[params['trim']]
				
				print( '\t' + threadOption +  ' count:', len(twtCol['payload'][params['input']]) )
				print('\t\twill extract max:', trim)

				expOrImp = params['output'][:3]
				expOrImp = expOrImp[0].upper() + expOrImp[1:]
				cacheFilename = './Caches/' + expOrImp +'TwttrThreads/' + self.cols['collectionTopic'] + '/' + 'threads.json'
				
				output[params['output']] = socMedGenCol.genExplThreadsCol(
					twtCol['payload'][params['input']][:trim], 
					settings, 
					cacheFilename,
					expThreadFlag=params['expFlag']
				)
			else:
				print('\t', threadOption, 'off, will read report cache')
				if( 'id' in settings ):
					cache = self.getColFromCache(name, settings['id'])
					if( params['output'] in cache ):
						output[params['output']] = cache[params['output']]
				else:
					print('\tcan\'t read cache, id not provided')

		
		#add degree 1 serp col - start
		print('\t adding degree 1 cols')
		deg1Settings = self.cols['degree-1-twt-cols']
		dedupSet = set()

		if( 'tweets' in output['serp']['payload'] ):

			#don't add a tweet already explored in degree 2
			for twt in output['serp']['payload']['tweets']:
				dedupSet.add(twt['data-tweet-id'])
	
			output['degree-1-twt-col'] = [{'name': name, 'tweet-links': []}]		
			ExtractMicroCol.addDegree1TwtLinks(
				output['serp']['payload']['tweets'], 
				output['degree-1-twt-col'][-1]['tweet-links'],
				dedupSet
			)
			
			print('\tdeg 1', name, 'active:', deg1Settings['active'][name] )
			if( deg1Settings['active'][name] ):
				cacheFilename = './Caches/Deg1Twttr/' + self.cols['collectionTopic'] + '/' + name + '.json'
				ExtractMicroCol.addTwDeg1Col(
					name, 
					output['degree-1-twt-col'][-1]['tweet-links'], 
					cacheFilename, 
					deg1Settings
				)

		for colOpt in ['explicit-thread-cols', 'implicit-thread-cols']:
			if( 'thread-cols' in output[colOpt] ):

				threadTypeName = name + '-' + colOpt[:3] + '-threads'
				output['degree-1-twt-col'].append( {'name': threadTypeName, 'tweet-links': []} )
				for threadCol in output[colOpt]['thread-cols']:
					if( 'tweets' not in threadCol ):
						continue

					ExtractMicroCol.addDegree1TwtLinks(
						threadCol['tweets'], 
						output['degree-1-twt-col'][-1]['tweet-links'],
						dedupSet
					)

				
				print('\tdeg 1', threadTypeName, 'active:', deg1Settings['active'][threadTypeName] )
				cacheFilename = './Caches/Deg1Twttr/' + self.cols['collectionTopic'] + '/' + threadTypeName + '.json'
				ExtractMicroCol.addTwDeg1Col(
					threadTypeName, 
					output['degree-1-twt-col'][-1]['tweet-links'], 
					cacheFilename, 
					deg1Settings
				)
		#add degree 1 serp col - end

		return output

	def genFacebookCols(self, name, settings):
		print('\ngenFacebookCols():')

		filename = settings['inputFileWithPosts'].split('/')[-1].replace('.json', '')
		fbCol = getDictFromFile( settings['inputFileWithPosts'] )
		fbCol = socMedGenCol.normalizeCol(fbCol)
		
		return fbCol

	def genScoopItCols(self, settings):
		print('\ngenScoopItCols():')
		
		output = {}
		if( 'scoops' in settings ):
			if( settings['scoops']['active'] ):
				
				output['scoops'] = scoopitSearch(
					query=settings['scoops']['query'], 
					SERPMaxPages=settings['scoops']['SERPMaxPages'], 
					postVerticalFlag=True
				)

		if( 'topics' in settings ):
			if( settings['topics']['active'] ):
				
				output['topics'] = scoopitSearch(
					query=settings['topics']['query'], 
					SERPMaxPages=settings['topics']['SERPMaxPages'], 
					postVerticalFlag=False, 
					topicMaxPages=settings['topics']['SERPMaxPages'], 
					extraParams=settings['topics']['extraParams']
				)

		return output

	def genSutoriCols(self, query, settings):
		print('\ngenSutoriCols()')
		output = sutoriSearch(query, maxStories=settings['maxStories'], chromedriverPath=settings['chromedriverPath'])
		return output

	def genTwitterMoments(self, name, uris, query):

		output = {
			'moments': [] 
		}
		
		for uri in uris:
			output['moments'].append( self.genTwitterMomentsSingle(name, uri, query) )

		return output

	def genTwitterMomentsSingle(self, name, uri, query):
		
		print('\ngenTwitterMoments():')

		uri = uri.strip()
		if( len(uri) == 0 ):
			return {}


		query = query.replace(' ', '-')
		tweets = twitterGetTweetFromMoment(uri)

		if( len(tweets) == 0 ):
			return {}

		twtCount = len(tweets['payload'])
		print('\textracted tweets count:', twtCount)	
	
		tweets['stats'] = {}
		tweets['stats']['tweet-links-dist'] = {}
		tweets['stats']['total-tweets'] = twtCount
		tweets['stats']['total-links'] = 0
		tweets['degree-1-twt-col'] = {'name': name, 'tweet-links': []}
		dedupSet = set()
		for i in range( twtCount ):

			linkCount = len( tweets['payload'][i]['tweet-links'] )
			tweets['stats']['tweet-links-dist'].setdefault(linkCount, 0)
			tweets['stats']['tweet-links-dist'][linkCount] += 1
			tweets['stats']['total-links'] += linkCount
			
			for link in tweets['payload'][i]['tweet-links']:
	
				tweetId = parseTweetURI( link )
				if( tweetId['id'] != '' and tweetId['id'] not in dedupSet ):

					parent = getTweetLink(tweets['payload'][i]['data-screen-name'], tweets['payload'][i]['data-tweet-id'])
					tweets['degree-1-twt-col']['tweet-links'].append( {'uri': link, 'parent': parent})
					dedupSet.add( tweetId['id'] )

		print('\t adding degree 1 cols')
		deg1Settings = self.cols['degree-1-twt-cols']
		print('\tdeg 1', name, 'active:', deg1Settings['active'][name] )
		
		if( deg1Settings['active'][name] ):
			cacheFilename = './Caches/Deg1Twttr/' + self.cols['collectionTopic'] + '/' + name + '.json'
			
			ExtractMicroCol.addTwDeg1Col(
				name, 
				tweets['degree-1-twt-col']['tweet-links'], 
				cacheFilename, 
				deg1Settings
			)

		return tweets

	@staticmethod
	def addTwDeg1Col(name, twts, cacheFilename, settings):

		print('\naddTwDeg1Col()')

		if( len(twts) == 0 ):
			return {}
		
		links = []
		uriParentDict = {}
		for i in range(len(twts)):
			
			twts[i]['uri'] = twts[i]['uri'].strip()
			twts[i]['output'] = {}
			links.append( twts[i]['uri'] )
			uriParentDict[ twts[i]['uri'] ] = {'parent': twts[i]['parent'], 'pos': i}

		trim = settings['config']['maxImpThreadToExplore']
	
		print('\taddtTwDeg1Col(), src on:', name)
		print('\ttweet links count:', len(links))
		print('\twill extract max:', trim)

		tCols = socMedGenCol.genExplThreadsCol(
			links[:trim], 
			settings['config'], 
			cacheFilename,
			expThreadFlag=False
		)

		if( 'thread-cols' not in tCols ):
			print('\tthread-cols absent, returning')
			return tCols

		for twtCol in tCols['thread-cols']:
			
			reqURI = twtCol['self']
			if( reqURI in uriParentDict ):
				pos = uriParentDict[reqURI]['pos']
				twts[pos]['output'] = twtCol
			
		return twts

	@staticmethod
	def cpAllSegCols(tgt, src, referer):
		
		if( 'segmented-cols' not in src ):
			return

		for seg in ['ss', 'ms', 'mm', 'mc']:
			tgt[seg] += src['segmented-cols'][seg]


	def extractCols(self):

		allSegmentedCols = {
			'name': 'all',
			'timestamp': getNowTime(),
			"extraction-timestamp": getNowTime(),
			'segmented-cols': {
				'ss': [],
				'sm': [],
				'ms': [],
				'mm': [],
				'mc': []
			}
		}

		print('\nextractCols() - start')
		
		for i in range(len(self.cols['sources'])):

			src = self.cols['sources'][i]
			print('\tsrc/active:', src['name'], src['active'])

			src.setdefault('config', {})
			#tranfer generic plot config to local src config
			src['config']['generic'] = self.cols['generic']
			

			if( src['active'] == False and 'id' in src ):
				self.cols['sources'][i]['output'] = self.getColFromCache( src['name'], src['id'] )

			if( src['name'] == 'reddit' ):
				
				if( src['active'] ):
					self.cols['sources'][i]['output'] = redditSearchExpand(src['query'], maxPages=src['config']['maxPages'], extraParams=src['config'])

				if( src['config']['expandDegree1Comments'] ):
					#allow update (saving source in cache) of source when deg 1 links have been explored
					src['active'] = True

				SegmentCols.genericAddReplyGroup( self.cols['sources'][i]['output'], SegmentCols.redditAuthorComp )
				segCols = SegmentCols.redditSegmentCols(self.cols['sources'][i]['output'], self.cols['collectionTopic'], src['id'], src['config']['sort'], extraParams=src['config'] )
				ExtractMicroCol.cpAllSegCols(allSegmentedCols['segmented-cols'], segCols, src['name'])

				segOutfilename = './Caches/SegmentedCols/' + self.cols['collectionTopic'] + '/' + src['id'] + '.json'
				dumpJsonToFile(segOutfilename, segCols, indentFlag=True)

			elif( src['name'] == 'wikipedia' ):

				if( src['active'] ):
					self.cols['sources'][i]['output'] = wikipediaGetExternalLinksDictFromPage(src['uri'])

			elif( src['name'] == 'twitter-serp' ):

				if( src['active'] ):
					self.cols['sources'][i]['output'] = self.genTwitterCols( src['name'], src['config'] )

				segCols = SegmentCols.twitterSegmentCols(self.cols['sources'][i]['output'], self.cols['collectionTopic'], src['id'], src['config']['vertical'], extraParams=src['config'])
				ExtractMicroCol.cpAllSegCols(allSegmentedCols['segmented-cols'], segCols, src['name'])

			elif( src['name'] == 'twitter-moments' ):

				if( src['active'] ):
					self.cols['sources'][i]['output'] = self.genTwitterMoments( src['name'], src['uris'], src['query'] )

				segCols = SegmentCols.twitterMomentsSegmentCols(self.cols['sources'][i]['output'], self.cols['collectionTopic'], src['id'], extraParams=src['config'])
				ExtractMicroCol.cpAllSegCols(allSegmentedCols['segmented-cols'], segCols, src['name'])

			elif( src['name'] == 'facebook' ):

				print('\tFB is off')
				'''
					if( src['active'] ):
						self.cols['sources'][i]['output'] = self.genFacebookCols( src['name'], src['config'] )
					
					SegmentCols.genericAddReplyGroup( self.cols['sources'][i]['output'], SegmentCols.facebookAuthorComp )
					SegmentCols.facebookSegmentCols(self.cols['sources'][i]['output'], self.cols['collectionTopic'], src['id'], extraParams=src['config'])
				'''
			elif( src['name'] == 'scoopit' ):

				if( src['active'] ):
					self.cols['sources'][i]['output'] = self.genScoopItCols( src['config'] )

				segCols = SegmentCols.scoopitSegmentCols(self.cols['sources'][i]['output'], self.cols['collectionTopic'], src['id'], extraParams=src['config'])
				ExtractMicroCol.cpAllSegCols(allSegmentedCols['segmented-cols'], segCols, src['name'])

			elif( src['name'] == 'sutori' ):

				if( src['active'] ):
					self.cols['sources'][i]['output'] = self.genSutoriCols( src['query'], src['config'] )

				segCols = SegmentCols.sutoriSegmentCols(self.cols['sources'][i]['output'], self.cols['collectionTopic'], src['id'], extraParams=src['config'])
				ExtractMicroCol.cpAllSegCols(allSegmentedCols['segmented-cols'], segCols, src['name'])

			elif( src['name'] == 'all' ):
				SegmentCols.genericSegmentCols( allSegmentedCols, self.cols['collectionTopic'], src['id'], extraParams=src['config'] )

			
			#save src in cache - start
			if( 'output' in self.cols['sources'][i] and src['active'] ):
				if( len(self.cols['sources'][i]['output']) != 0 ):
					tmpSrcCacheFilename = './Caches/Sources/' + self.cols['collectionTopic'] + '/' + src['id'] + '.json'
					dumpJsonToFile(tmpSrcCacheFilename, self.cols['sources'][i]['output'])
			#save src in cache - end
			print()

		print('extractCols() - end\n')
		return self.cols

def main(configFilename):
	
	configFilename = configFilename.strip()
	if( len(configFilename) == 0 ):
		return

	prevNow = datetime.now()

	mc = ExtractMicroCol(configFilename)
	#print('SWITCHED OF WRITING REPORT')
	mc.writeReport(False)

	delta = datetime.now() - prevNow
	print('\tdelta seconds:', delta.seconds)

if __name__ == "__main__":
	if( len(sys.argv) == 2 ):
		main( sys.argv[1] )
	else:
		print('\nUsage:')
		print('\t', sys.argv[0], 'config.json')
