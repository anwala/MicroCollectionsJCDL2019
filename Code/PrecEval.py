import os, sys

from copy import deepcopy
from datetime import datetime

from genericCommon import getDictFromFile
from genericCommon import readTextFromFile
from genericCommon import getURIHash
from genericCommon import dumpJsonToFile
from genericCommon import getNowTime
#from genericCommon import dereferenceURI
from genericCommon import parallelTask
from genericCommon import writeTextToFile
from genericCommon import clean_html
from genericCommon import DocVect
from genericCommon import extractPageTitleFromHTML
from genericCommon import mimicBrowser

class PrecEval(object):

	def __init__(self, goldstandardFilename):

		self.goldstandardFilename = goldstandardFilename
		self.goldstandard = getDictFromFile(goldstandardFilename)
		self.simCoeff = -1
		
		if( 'sim-coeff' in self.goldstandard ):
			self.simCoeff = self.goldstandard['sim-coeff']
		elif( 'uris' in self.goldstandard ):
			
			PrecEval.getHTMLAndTextForURILst( self.goldstandard, self.goldstandardFilename )
			self.setSimCoeff()
			#parallel version did not achieve decent speedup
			#self.prlSetSimCoeff()
		else:
			print('\tInvalid goldstandard supplied')
			print('\t', self.goldstandard)
		
	@staticmethod
	def calcPairSim(matrix, noopFlag):
		
		if( len(matrix) != 2 or noopFlag ):
			return -1

		params = {}
		params['normalize'] = True
		matrix = DocVect.getTFMatrixFromDocList( matrix, params=params )

		if( len(matrix) != 2 ):
			return -1
		
		return DocVect.cosineSim(matrix[0], matrix[1])

	@staticmethod
	def uriDctHasBasics(uriDct):

		if( 'text' in uriDct and 'text-len' in uriDct and 'title' in uriDct and 'status-code' in uriDct ):
			return True
		else:
			return False

	@staticmethod
	def getHTMLAndTextForURILst(col, outfilename=None, printSuffix='', extraParams=None):

		if( extraParams is None ):
			extraParams = {}

		extraParams.setdefault('simCacheLookup', True)

		jobsLst = []
		statusCodeJobsLst = []
		jobSize = len(col['uris'])
		for i in range(jobSize):

			uri = col['uris'][i]

			if( 'hash' not in uri ):
				uri['hash'] = getURIHash( uri['uri'] )


			if( PrecEval.uriDctHasBasics(uri) and extraParams['simCacheLookup'] ):
				#ignore already proc. files, usually already proc. segments
				#except cache lookup is off
				continue

			#attempt - cache - start
			cosineSimFile = './Caches/CosineSim/' + col['uris'][i]['hash'] + '.json'
			if( os.path.exists(cosineSimFile) and extraParams['simCacheLookup'] ):
				
				cache = getDictFromFile(cosineSimFile)
				if( PrecEval.uriDctHasBasics(cache) ):
					uri['text'] = cache['text']
					uri['text-len'] = cache['text-len']
					uri['title'] = cache['title']
					uri['status-code'] = cache['status-code']
					#print('\t\tskipping since cache available')
					continue


			if( 'custom' in uri ):
				if( 'mime' in uri['custom'] ):
					if( uri['custom']['mime'] != 'text/html' ):
						
						print('\tskipping', uri['custom']['mime'])
						uri['text'] = 'NoneHTML'
						uri['text-len'] = 8

						uri.setdefault('title', '')
						uri.setdefault('status-code', -1)
						continue
			'''		
				txtFile = './Caches/Plaintext/' + uri['hash'] + '.txt'
				htmlFile = './Caches/HTML/' + uri['hash'] + '.html'
				if( os.path.exists(txtFile) ):
					uri['text'] = readTextFromFile(txtFile)
					uri['text-len'] = len(uri['text'])
					uri['title'] = extractPageTitleFromHTML( readTextFromFile(htmlFile) )
					continue
			'''
			#attempt - cache - end

			jobsLst.append({
				'func': mimicBrowser, 
				'args': {'uri': uri['uri'], 'extraParams': {'sizeRestrict': 4000000}}, 
				'misc': {'i': i, 'hash': uri['hash']}, 
				'print': 'gtHTML.URILst->dfURI(): ' + str(i) + ' of ' + str(jobSize) + printSuffix#+ '\n\tu: ' + uri['uri']
			})

			statusCodeJobsLst.append({
				'func': mimicBrowser,
				'args': {'uri': uri['uri'], 'getRequestFlag': False, 'extraParams': None},
				'misc': {'i': i, 'hash': uri['hash']},
				'print': 'gtHTML.URILst->mkHdReq.(): ' + str(i) + ' of ' + str(jobSize) + printSuffix
			})

	
		resLst = []
		if( len(jobsLst) != 0 ):
			resLst = parallelTask(jobsLst, threadCount=3)

		for res in resLst:
			
			html = res['output']
			plaintext = clean_html(html)
			indx = res['misc']['i']

			col['uris'][indx]['text'] = plaintext
			col['uris'][indx]['text-len'] = len(plaintext)
			col['uris'][indx]['title'] = extractPageTitleFromHTML(html)

			writeTextToFile('./Caches/HTML/' + res['misc']['hash'] + '.html', html)
			print('\t\thtmllen:', len(html))
			writeTextToFile('./Caches/Plaintext/' + res['misc']['hash'] + '.txt', plaintext)
			print('\t\tplaintextlen:', len(plaintext))

		
		resLst = []
		if( len(statusCodeJobsLst) != 0 ):
			resLst = parallelTask(statusCodeJobsLst, threadCount=3)

		for res in resLst:
			
			headReq = res['output']
			indx = res['misc']['i']

			cache = {}
			cache['text'] = col['uris'][indx]['text']
			cache['text-len'] = col['uris'][indx]['text-len']
			cache['title'] = col['uris'][indx]['title']
			cache['status-code'] = -1

			col['uris'][indx]['status-code'] = -1
			if( 'status-code' in headReq ):
				cache['status-code'] = headReq['status-code']
				col['uris'][indx]['status-code'] = headReq['status-code']


			cacheFilename = './Caches/CosineSim/' + res['misc']['hash'] + '.json'
			dumpJsonToFile(cacheFilename, cache)

		col['timestamp'] = getNowTime()
		if( outfilename is not None ):
			dumpJsonToFile(outfilename, col)


	@staticmethod
	def combineDocsForIndices(uris, indices):

		combinedDoc = ''

		for indx in indices:
			combinedDoc += uris[indx]['text'] + '\n\n'

		return combinedDoc

	def updateGoldstandard(self):
		self.goldstandard['timestamp'] = getNowTime()
		dumpJsonToFile(self.goldstandardFilename, self.goldstandard)

	def setSimCoeff(self):

		goldSize = len(self.goldstandard['uris'])
		if( goldSize == 0 ):
			return

		avgSim = 0
		validDocSize = 0
		for i in range( goldSize ):
			
			oneDoc = self.goldstandard['uris'][i]['text']
			locRange = deepcopy( list(range(len(self.goldstandard['uris']))) )
			locRange.remove(i)
			
			restDoc = PrecEval.combineDocsForIndices(self.goldstandard['uris'], locRange)
			docList = [oneDoc]
			docList.append(restDoc)

			params = {}
			params['normalize'] = True
			locMatrix = DocVect.getTFMatrixFromDocList( docList, params=params )

			self.goldstandard['uris'][i]['sim'] = -1
			if( len(locMatrix) != 2 ):
				print('\tskipping bad entry')
				continue

			sim = DocVect.cosineSim(locMatrix[0], locMatrix[1])
			self.goldstandard['uris'][i]['sim'] = sim
			
			avgSim += sim
			validDocSize += 1

			print('\t', i, 'of', goldSize, 'vs rest =', sim, 'sim-coeff:', avgSim/validDocSize)
			

		if( validDocSize > 0 ):
			avgSim = avgSim/validDocSize
		else:
			avgSim = -1

		self.goldstandard['sim-coeff'] = avgSim
		self.simCoeff = avgSim
		self.updateGoldstandard()

		print('\tavgSim:', avgSim)


	@staticmethod
	def isRel(sim, goldSim):
		
		sim = round(sim, 1)
		goldSim = round(goldSim, 1)

		if( sim >= goldSim ):
			return True
		else:
			return False

	@staticmethod
	def prlEvalCol(col, goldstandard, removeTxt=True, extraParams=None):
		
		if( extraParams is None ):
			extraParams = {}

		extraParams.setdefault('minTextSize', 300)
		'''
			Important note:
			1. If minTextSize is changed, If gold standard text content is change, 
				set simCacheLookup False avoid cache lookup in order to true to recalculate sim
			
			2. If gold standard sim-coeff is change, no need to do anything
		'''
		extraParams.setdefault('simCacheLookup', True)
		extraParams.setdefault('printSuffix', '')


		colsize = len(col['uris'])

		if( colsize == 0 or len(goldstandard) == 0 ):
			print('\tprlEvalCol(): colsize is 0 or goldstandard == 0, returning')
			return -1

		if( 'uris' not in goldstandard ):
			print('\tprlEvalCol(): no uris in goldstandard, returning')
			return -1

		goldRange = list(range(len(goldstandard['uris'])))
		combinedGold = PrecEval.combineDocsForIndices(goldstandard['uris'], goldRange)
		
		precision = 0
		validColSize = 0
		jobsLst = []		
		for i in range(colsize):			

			#attempt getting sim from cache - start
			cosineSimFile = './Caches/CosineSim/' + col['uris'][i]['hash'] + '.json'
			if( os.path.exists(cosineSimFile) and extraParams['simCacheLookup'] ):
				
				cosSim = getDictFromFile(cosineSimFile)
				if( 'sim' in cosSim ):
					
					col['uris'][i]['sim'] = cosSim['sim']

					if( cosSim['sim'] != -1 ):
						validColSize += 1

						if( PrecEval.isRel(cosSim['sim'], goldstandard['sim-coeff']) ):
							col['uris'][i]['relevant'] = True
							precision += 1
						else:
							col['uris'][i]['relevant'] = False

					continue
			#attempt getting sim from cache - end

			noopFlag = False
			usingSubText = ''
			if( len(col['uris'][i]['text']) < extraParams['minTextSize'] ):
				if( 'post-details' in col['uris'][i] ):
					#gold standards do not have post-details
					if( 'substitute-text' in col['uris'][i]['post-details'] ):
						
						subText = col['uris'][i]['post-details']['substitute-text'].strip()
						if( subText != '' ):
							col['uris'][i]['text'] = subText
							col['uris'][i]['custom']['substitute-text-active'] = True
							usingSubText = '\n\t\tusing subtext: ' + col['uris'][i]['uri']
						else:
							noopFlag = True
					
					else:
						#don't process uris with small text
						#don't skip (continue) so cache can update
						noopFlag = True

			matrix = [
				col['uris'][i]['text'], 
				combinedGold
			]
			keywords = {'matrix': matrix, 'noopFlag': noopFlag}
			toPrint = '\tprlEvalCol():' + str(i) + ' of ' + str(colsize) + ' ' + extraParams['printSuffix'] + usingSubText

			if( 'status-code' not in col['uris'][i] ):
				print('\tproblem ahead for uri:', col['uris'][i]['uri'])
				print('\tproblem ahead for hash:', col['uris'][i]['hash'])
				print('\tproblem ahead for cosineSimFile:', cosineSimFile)
				print('\tproblem ahead for keys:', col['uris'][i].keys())


			cache = {
				'hash': col['uris'][i]['hash'],
				'self': cosineSimFile,
				'uri': col['uris'][i]['uri'],
				'title': col['uris'][i]['title'],
				'text': col['uris'][i]['text'],
				'text-len': len(col['uris'][i]['text']),
				'status-code': col['uris'][i]['status-code']
			}
			jobsLst.append({
				'func': PrecEval.calcPairSim, 
				'args': keywords, 
				'misc': {'i': i, 'cache': cache}, 
				'print': toPrint
			})

		resLst = []
		if( len(jobsLst) != 0 ):
			resLst = parallelTask(jobsLst, threadCount=3)
		
		for res in resLst:
			
			indx = res['misc']['i']
			cache = res['misc']['cache']

			sim = res['output']
			col['uris'][indx]['sim'] = sim

			if( sim != -1 ):
				validColSize += 1

				if( PrecEval.isRel(sim, goldstandard['sim-coeff']) ):
					col['uris'][indx]['relevant'] = True
					precision += 1
				else:
					col['uris'][indx]['relevant'] = False

			#write cache - start
			cache['sim'] = sim
			dumpJsonToFile(cache['self'], cache)
			#write cache - end
		

		if( removeTxt ):
			for i in range(colsize):
				if( 'text' in col['uris'][i] ):
					del col['uris'][i]['text']
		

		if( validColSize > 0 ):
			return precision/validColSize
		else:
			return -1


	@staticmethod
	def singleDocPrecCalc(docList):

		if( len(docList) != 2 ):
			return -1

		params = {}
		params['normalize'] = True
		locMatrix = DocVect.getTFMatrixFromDocList( docList, params=params )

		if( len(locMatrix) != 2 ):
			return -1

		return DocVect.cosineSim(locMatrix[0], locMatrix[1])

def precEvalCol(goldstandard, testCol=None, testColFilename=None, removeTxt=True, printSuffix='', extraParams=None):
	
	if( isinstance(goldstandard, str) ):
		goldstandard = goldstandard.strip()
		if( len(goldstandard) == 0 ):
			return {}

		gs = PrecEval(goldstandard)
		goldstandard = gs.goldstandard

	if( extraParams is None ):
		extraParams = {}


	if( len(goldstandard) != 0 ):	
		PrecEval.getHTMLAndTextForURILst(testCol, testColFilename, printSuffix=printSuffix, extraParams=extraParams)

		extraParams['printSuffix'] = printSuffix
		testCol['predicted-precision'] = PrecEval.prlEvalCol(testCol, goldstandard, removeTxt=removeTxt, extraParams=extraParams)
		testCol['sim-coeff'] = goldstandard['sim-coeff']
		return testCol

	return {}
		

def main(goldFilname, testFilename=None):	

	goldFilname = goldFilname.strip()
	if( len(goldFilname) == 0 ):
		return

	prevNow = datetime.now()
	goldstandard = PrecEval(goldFilname)

	if( testFilename is not None ):
		tstFile = getDictFromFile(testFilename)
		PrecEval.getHTMLAndTextForURILst(tstFile, testFilename)

		tstFile['timestamp'] = getNowTime()
		tstFile['predicted-precision'] = PrecEval.prlEvalCol(tstFile, goldstandard.goldstandard, removeTxt=False)
		tstFile['sim-coeff'] = goldstandard.simCoeff
		dumpJsonToFile(testFilename, tstFile)	

	delta = datetime.now() - prevNow
	print('\tdelta seconds:', delta.seconds)

if __name__ == "__main__":
	
	print('\nPrecEval()')

	if( len(sys.argv) == 2 ):
		print('\tGoldstandard')
		main( sys.argv[1] )
	elif( len(sys.argv) == 3 ):
		print('\tGoldstandard vs Test')
		main( sys.argv[1], sys.argv[2] )
	else:
		print('\nUsage:')
		print('\t', sys.argv[0], 'goldstandard.json [fileToEval.json]')