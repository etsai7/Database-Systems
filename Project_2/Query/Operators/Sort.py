from Catalog.Schema import DBSchema
from Query.Operator import Operator
from collections import deque

# Operator for External Sort
class Sort(Operator):

	def __init__(self, subPlan, **kwargs):
		super().__init__(**kwargs)
		self.subPlan		 = subPlan
		self.sortKeyFn	 = kwargs.get("sortKeyFn", None)
		self.sortKeyDesc = kwargs.get("sortKeyDesc", None)
		
		self.tempFileHash = dict()
		self.outputPageHash = dict()
		
		self.tempFile = None
		
		if self.sortKeyFn is None or self.sortKeyDesc is None:
			raise ValueError("No sort key extractor provided to a sort operator")

	# Returns the output schema of this operator
	def schema(self):
		return self.subPlan.schema()

	# Returns any input schemas for the operator if present
	def inputSchemas(self):
		return [self.subPlan.schema()]

	# Returns a string describing the operator type
	def operatorType(self):
		return "Sort"

	# Returns child operators if present
	def inputs(self):
		return [self.subPlan]


	# Iterator abstraction for external sort operator.
	def __iter__(self):
		self.outputIterator = self.processAllPages()
		return self

	def __next__(self):
		
		if self.pipelined:
			return self.outputPage()

		else:
			return next(self.outputIterator)

	# Page processing and control methods

	# Page-at-a-time operator processing
	def processInputPage(self, pageId, page):
		self.count += 1
		for Tuple in page:
			val = self.loadSchema(self.subPlan.schema(), Tuple)
			outputTuple = self.subPlan.schema().instantiate(*[val[f] for f in self.subPlan.schema().fields])
			self.emitOutputTupleHash(self.subPlan.schema().pack(outputTuple), int(self.count / self.M))
			
	# Set-at-a-time operator processing
	def processAllPages(self):
		self.count = 0;
		buf = self.storage.bufferPool
		self.M = buf.numFreePages() - 2
		for (PageId, Page) in iter(self.subPlan):
			self.processInputPage(PageId, Page)
		
		for k in list(self.outputPageHash.keys()):
			for pinfo in list(self.outputPageHash[k]):
				page = self.storage.bufferPool.getPage(pinfo[0])
				#self.printerr('asfjkasflajfklafj')
				#self.printerr(page.header.numTuples())
				l = list()
				for tup in page:
					val = self.loadSchema(self.subPlan.schema(), tup)
					outputTuple = self.subPlan.schema().instantiate(*[val[f] for f in self.subPlan.schema().fields])
					#self.printerr(outputTuple)
					l.append(outputTuple)
			l.sort(key=self.sortKeyFn)
			l = l[::-1]
			
			relId = self.getRelId(k)
			self.storage.removeRelation(relId)
			del self.tempFileHash[k]
			del self.outputPageHash[k]
			#self.printerr('numKeys: ' + str(self.outputPageHash.keys()))
			
			for tup in l:
				#self.printerr(tup)
				self.emitOutputTupleHash(self.subPlan.schema().pack(tup), k)
			
			
		while len(self.outputPageHash.keys()) > 1:
			prevk = None
			for k in list(self.outputPageHash.keys()):
				if prevk is not None:
					rl = deque()
					ll = deque()
					for pinfo in list(self.outputPageHash[k]):
						page = self.storage.bufferPool.getPage(pinfo[0])
						for tup in page:
							val = self.loadSchema(self.subPlan.schema(), tup)
							outputTuple = self.subPlan.schema().instantiate(*[val[f] for f in self.subPlan.schema().fields])
							rl.append(outputTuple)
					for pinfo in list(self.outputPageHash[prevk]):
						page = self.storage.bufferPool.getPage(pinfo[0])
						for tup in page:
							val = self.loadSchema(self.subPlan.schema(), tup)
							outputTuple = self.subPlan.schema().instantiate(*[val[f] for f in self.subPlan.schema().fields])
							ll.append(outputTuple)
					relId = self.getRelId(k)
					self.storage.removeRelation(relId)
					del self.tempFileHash[k]
					del self.outputPageHash[k]
					relId = self.getRelId(prevk)
					self.storage.removeRelation(relId)
					del self.tempFileHash[prevk]
					del self.outputPageHash[prevk]
					
					for i in range(len(rl) + len(ll)):
						if len(ll) == 0 or self.sortKeyFn(ll[0]) < self.sortKeyFn(rl[0]):
							self.emitOutputTupleHash(self.subPlan.schema().pack(rl.popleft()), k)
						else:
							self.emitOutputTupleHash(self.subPlan.schema().pack(ll.popleft()), k)
					prevk = None
				else:
					prevk = k
		#self.printerr('lalsllasdlsa')
		for k in list(self.outputPageHash.keys()):
			for pinfo in list(self.outputPageHash[k]):
				page = self.storage.bufferPool.getPage(pinfo[0])
				for tup in page:
					val = self.loadSchema(self.subPlan.schema(), tup)
					outputTuple = self.subPlan.schema().instantiate(*[val[f] for f in self.subPlan.schema().fields])
					#self.printerr('FinalOut:' + str(outputTuple))
					self.emitOutputTuple(self.subPlan.schema().pack(outputTuple))
					
		return self.storage.pages(self.relationId())
	# Plan and statistics information

	# Returns a single line description of the operator.
	def explain(self):
		return super().explain() + "(sortKeyDesc='" + str(self.sortKeyDesc) + "')"
	
	def getRelId(self, hashVal):
		return self.relationId() + 'temp' + str(hashVal)
		
	def initializeOutputHash(self, hashVal):
		relId = self.getRelId(hashVal)

		if self.storage.hasRelation(relId):
			self.storage.removeRelation(relId)

		self.storage.createRelation(relId, self.subPlan.schema())
		self.tempFileHash[hashVal] = self.storage.fileMgr.relationFile(relId)[1]
		self.outputPageHash[hashVal] = []
		
	def emitOutputTupleHash(self, tupleData, hashVal):
		if hashVal not in self.tempFileHash.keys():
			self.initializeOutputHash(hashVal)
		#self.printerr(tupleData)
		self.currFile = self.tempFileHash[hashVal]
		self.currOutputPages = self.outputPageHash[hashVal]

		allocatePage = not(self.outputPageHash[hashVal] and (self.outputPageHash[hashVal])[-1][1].header.hasFreeTuple())
		if allocatePage:
			# Flush the most recently updated output page, which updates the storage file's
			# free page list to ensure correct new page allocation.
			if self.outputPageHash[hashVal]:
				self.storage.bufferPool.flushPage((self.outputPageHash[hashVal])[-1][0])
			outputPageId = self.currFile.availablePage()
			outputPage	 = self.storage.bufferPool.getPage(outputPageId)
			self.outputPageHash[hashVal].append((outputPageId, outputPage))
		else:
			outputPage = (self.outputPageHash[hashVal])[-1][1]

		outputPage.insertTuple(tupleData)
		#self.printerr(outputPage.header.numTuples())

		if self.sampled:
			self.estimatedCardinality += 1
		else:
			self.actualCardinality += 1
			
	def printerr(self, string):
		f = open('err.txt', 'a')
		f.write(str(string) + '\n')
		f.close()