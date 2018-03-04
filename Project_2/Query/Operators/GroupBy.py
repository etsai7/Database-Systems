from Catalog.Schema import DBSchema
from Query.Operator import Operator
from collections import namedtuple

class GroupBy(Operator):
	def __init__(self, subPlan, **kwargs):
		super().__init__(**kwargs)

		if self.pipelined:
			raise ValueError("Pipelined group-by-aggregate operator not supported")

		self.subPlan		 = subPlan
		self.subSchema	 = subPlan.schema()
		self.groupSchema = kwargs.get("groupSchema", None)
		self.aggSchema	 = kwargs.get("aggSchema", None)
		self.groupExpr	 = kwargs.get("groupExpr", None)
		self.aggExprs		= kwargs.get("aggExprs", None)
		self.groupHashFn = kwargs.get("groupHashFn", None)

		self.validateGroupBy()
		self.initializeSchema()
		
		self.tempFileHash = dict()
		self.outputPageHash = dict()
		
		self.tempFile = None
		
		#self.outputSchema = self.aggSchema

	# Perform some basic checking on the group-by operator's parameters.
	def validateGroupBy(self):
		requireAllValid = [self.subPlan, \
											 self.groupSchema, self.aggSchema, \
											 self.groupExpr, self.aggExprs, self.groupHashFn ]

		if any(map(lambda x: x is None, requireAllValid)):
			raise ValueError("Incomplete group-by specification, missing a required parameter")

		if not self.aggExprs:
			raise ValueError("Group-by needs at least one aggregate expression")

		if len(self.aggExprs) != len(self.aggSchema.fields):
			raise ValueError("Invalid aggregate fields: schema mismatch")

	# Initializes the group-by's schema as a concatenation of the group-by
	# fields and all aggregate fields.
	def initializeSchema(self):
		schema = self.operatorType() + str(self.id())
		fields = self.groupSchema.schema() + self.aggSchema.schema()
		self.outputSchema = DBSchema(schema, fields)

	# Returns the output schema of this operator
	def schema(self):
		return self.outputSchema

	# Returns any input schemas for the operator if present
	def inputSchemas(self):
		return [self.subPlan.schema()]

	# Returns a string describing the operator type
	def operatorType(self):
		return "GroupBy"

	# Returns child operators if present
	def inputs(self):
		return [self.subPlan]

	# Iterator abstraction for selection operator.
	def __iter__(self):
		self.iterator = iter(self.subPlan)
		self.acc = dict()
		self.outputIterator = self.processAllPages()
		return self

	def __next__(self):
		if self.pipelined:
			return self.outputPage()

		else:
			return next(self.outputIterator)
		raise StopIteration
		'''
		(PageId, Page) = next(self.iterator)
		
			#self.printerr('memes')
			#self.printerr(self.schema().schema())
			#for k in self.acc.keys():
			#	self.emitOutputTuple(self.aggExprs[2](self.acc[k]))
			#raise StopIteration
		
		for Tuple in Page:
			# Load the lhs once per inner loop.
			val = self.loadSchema(self.subSchema, Tuple)
			
			temp = namedtuple('temp',val.keys())
			
			l = list()
			for k in val.keys():
				l.append(val[k])
			
			ntup = temp._make(l)
			
			expr = self.groupExpr(ntup)
			#self.printerr(expr)
			hash = self.groupHashFn((expr,0))
			#self.printerr(hash)
			#if hash not in self.acc.keys():
			#	self.acc[hash] = self.aggExprs[0]
			#self.acc[hash] = self.aggExprs[1](self.acc[hash],val)
			
			#ids = []
			#for tup in self.groupSchema.schema():
			#	ids.append(tup[0])
			#temp = namedtuple('temp',ids)
			#l = list()
			#for k in ids:
			#	l.append(val[k])
			#ntup = temp._make(l)
			#self.printerr(hash)
			self.emitOutputTupleHash(ntup, hash)'''

	# Page-at-a-time operator processing
	def processInputPage(self, pageId, page):
		raise ValueError("Page-at-a-time processing not supported for joins")

	# Set-at-a-time operator processing
	def processAllPages(self):
		self.initializeSchema()
		self.acc = dict()
		for (PageId, Page) in iter(self.subPlan):
			for Tuple in Page:
				# Load the lhs once per inner loop.
				val = self.loadSchema(self.subSchema, Tuple)
				
				ntup = self.subSchema.instantiate(*[val[f] for f in self.subSchema.fields])
				
				expr = self.groupExpr(ntup)
				#self.printerr(expr)
				hash = self.groupHashFn((expr,0))

				self.emitOutputTupleHash(Tuple, hash)
		for k in self.outputPageHash.keys():
			acc = dict()
			for i,outSchema in enumerate(self.aggSchema.schema()):
				acc[outSchema[0]] = self.aggExprs[i][0]
			for pinfo in self.outputPageHash[k]:
				page = self.storage.bufferPool.getPage(pinfo[0])
				for tup in page:
					val = self.loadSchema(self.subSchema, tup)
					
					temp = namedtuple('temp',val.keys())
					l = list()
					for k in val.keys():
						l.append(val[k])
					ntup = temp._make(l)
					for i,outSchema in enumerate(self.groupSchema.schema()):
						acc[outSchema[0]] = self.groupExpr(ntup)
					for i,outSchema in enumerate(self.aggSchema.schema()):
						acc[outSchema[0]] = self.aggExprs[i][2](self.aggExprs[i][1](acc[outSchema[0]], ntup))
			outputTuple = self.outputSchema.instantiate(*[acc[f] for f in self.outputSchema.fields])
			#self.printerr(outputTuple)
			self.emitOutputTuple(self.outputSchema.pack(outputTuple))
			#if self.outputPages:
				#self.outputPages = [self.outputPages[-1]]
					
		return self.storage.pages(self.relationId())
				
	
	def getRelId(self, hashVal):
		return self.relationId() + 'temp' + str(hashVal)
		
	def initializeOutputHash(self, hashVal):
		relId = self.getRelId(hashVal)

		if self.storage.hasRelation(relId):
			self.storage.removeRelation(relId)

		self.storage.createRelation(relId, self.subSchema)
		self.tempFileHash[hashVal] = self.storage.fileMgr.relationFile(relId)[1]
		self.outputPageHash[hashVal] = []
		
	def emitOutputTupleHash(self, tupleData, hashVal):
		if hashVal not in self.tempFileHash.keys():
			self.initializeOutputHash(hashVal)
			
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

		#if self.sampled:
		#	self.estimatedCardinality += 1
		#else:
		#	self.actualCardinality += 1

	def printerr(self, string):
		f = open('err.txt', 'a')
		f.write(str(string) + '\n')
		f.close()
	# Plan and statistics information

	# Returns a single line description of the operator.
	def explain(self):
		return super().explain() + "(groupSchema=" + self.groupSchema.toString() \
														 + ", aggSchema=" + self.aggSchema.toString() + ")"
