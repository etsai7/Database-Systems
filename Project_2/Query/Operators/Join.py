import itertools

from Catalog.Schema import DBSchema
from Query.Operator import Operator

class Join(Operator):
	def __init__(self, lhsPlan, rhsPlan, **kwargs):
		super().__init__(**kwargs)

		if self.pipelined:
			raise ValueError("Pipelined join operator not supported")

		self.lhsPlan		= lhsPlan
		self.rhsPlan		= rhsPlan
		self.joinExpr	 = kwargs.get("expr", None)
		self.joinMethod = kwargs.get("method", None)
		self.lhsSchema	= kwargs.get("lhsSchema", None if lhsPlan is None else lhsPlan.schema())
		self.rhsSchema	= kwargs.get("rhsSchema", None if rhsPlan is None else rhsPlan.schema())

		self.lhsKeySchema	 = kwargs.get("lhsKeySchema", None)
		self.rhsKeySchema	 = kwargs.get("rhsKeySchema", None)
		self.lhsHashFn			= kwargs.get("lhsHashFn", None)
		self.rhsHashFn			= kwargs.get("rhsHashFn", None)

		self.validateJoin()
		self.initializeSchema()
		self.initializeMethod(**kwargs)
		
		self.pidsInBlock = list()
		
		self.tempFileHashR = dict()
		self.outputPageHashR = dict()
		
		self.tempFileHashL = dict()
		self.outputPageHashL = dict()
		
		self.tempFile = None

	# Checks the join parameters.
	def validateJoin(self):
		# Valid join methods: "nested-loops", "block-nested-loops", "indexed", "hash"
		if self.joinMethod not in ["nested-loops", "block-nested-loops", "indexed", "hash"]:
			raise ValueError("Invalid join method in join operator")

		# Check all fields are valid.
		if self.joinMethod == "nested-loops" or self.joinMethod == "block-nested-loops":
			methodParams = [self.joinExpr]

		elif self.joinMethod == "indexed":
			methodParams = [self.lhsKeySchema]

		elif self.joinMethod == "hash":
			methodParams = [self.lhsHashFn, self.lhsKeySchema, \
											self.rhsHashFn, self.rhsKeySchema]

		requireAllValid = [self.lhsPlan, self.rhsPlan, \
											 self.joinMethod, \
											 self.lhsSchema, self.rhsSchema ] \
											 + methodParams

		if any(map(lambda x: x is None, requireAllValid)):
			raise ValueError("Incomplete join specification, missing join operator parameter")

		# For now, we assume that the LHS and RHS schema have
		# disjoint attribute names, enforcing this here.
		for lhsAttr in self.lhsSchema.fields:
			if lhsAttr in self.rhsSchema.fields:
				raise ValueError("Invalid join inputs, overlapping schema detected")


	# Initializes the output schema for this join.
	# This is a concatenation of all fields in the lhs and rhs schema.
	def initializeSchema(self):
		schema = self.operatorType() + str(self.id())
		fields = self.lhsSchema.schema() + self.rhsSchema.schema()
		self.joinSchema = DBSchema(schema, fields)

	# Initializes any additional operator parameters based on the join method.
	def initializeMethod(self, **kwargs):
		if self.joinMethod == "indexed":
			self.indexId = kwargs.get("indexId", None)
			if self.indexId is None or self.lhsKeySchema is None:
				raise ValueError("Invalid index for use in join operator")

	# Returns the output schema of this operator
	def schema(self):
		return self.joinSchema

	# Returns any input schemas for the operator if present
	def inputSchemas(self):
		return [self.lhsSchema, self.rhsSchema]

	# Returns a string describing the operator type
	def operatorType(self):
		readableJoinTypes = { 'nested-loops'			 : 'NL'
												, 'block-nested-loops' : 'BNL'
												, 'indexed'						: 'Index'
												, 'hash'							 : 'Hash' }
		return readableJoinTypes[self.joinMethod] + "Join"

	# Returns child operators if present
	def inputs(self):
		return [self.lhsPlan, self.rhsPlan]

	# Iterator abstraction for join operator.
	def __iter__(self):
		self.initializeOutput()
		self.inputIteratorL = iter(self.lhsPlan)
		self.inputFinished = False

		if not self.pipelined:
			if self.joinMethod == 'hash':
				self.outputIterator = self.hashJoin()
			elif self.joinMethod == 'block-nested-loops':
				self.outputIterator = self.blockNestedLoops()
			elif self.joinMethod == 'nested-loops':
				self.outputIterator = self.nestedLoops()

		return self

	def __next__(self):
		self.inputIteratorR = iter(self.rhsPlan)
		if self.pipelined:
			while not(self.inputFinished or self.isOutputPageReady()):
				try:
					lPageId, lhsPage = next(self.inputIteratorL)
					for lTuple in lhsPage:
						compare(lTuple)
						if self.outputPages:
							self.outputPages = [self.outputPages[-1]]
				except StopIteration:
					self.inputFinished = True
			return self.outputPage()

		else:
			return next(self.outputIterator)

	# Page-at-a-time operator processing
	def processInputPage(self, pageId, page):
		raise ValueError("Page-at-a-time processing not supported for joins")

	# Set-at-a-time operator processing
	def processAllPages(self):
		if self.joinMethod == "nested-loops":
			return self.nestedLoops()

		elif self.joinMethod == "block-nested-loops":
			return self.blockNestedLoops()

		elif self.joinMethod == "indexed":
			return self.indexedNestedLoops()

		elif self.joinMethod == "hash":
			return self.hashJoin()

		else:
			raise ValueError("Invalid join method in join operator")


	##################################
	#
	# Nested loops implementation
	#
	def nestedLoops(self):
		for (lPageId, lhsPage) in iter(self.lhsPlan):
			for lTuple in lhsPage:
				# Load the lhs once per inner loop.
				joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)
                
				for (rPageId, rhsPage) in iter(self.rhsPlan):
					for rTuple in rhsPage:
						# Load the RHS tuple fields.
						joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))
                
						# Evaluate the join predicate, and output if we have a match.
						if eval(self.joinExpr, globals(), joinExprEnv):
							outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
							self.emitOutputTuple(self.joinSchema.pack(outputTuple))
                
				# No need to track anything but the last output page when in batch mode.
				#compare(lTuple)
				if self.outputPages:
					self.outputPages = [self.outputPages[-1]]

		# Return an iterator to the output relation
		return self.storage.pages(self.relationId())

	def compare(lTuple):
		# Load the lhs once per inner loop.
		joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)

		for (rPageId, rhsPage) in iter(self.rhsPlan):
			for rTuple in rhsPage:
				# Load the RHS tuple fields.
				joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))

				# Evaluate the join predicate, and output if we have a match.
				if eval(self.joinExpr, globals(), joinExprEnv):
					outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
					self.emitOutputTuple(self.joinSchema.pack(outputTuple))

	##################################
	#
	# Block nested loops implementation
	#
	# This attempts to use all the free pages in the buffer pool
	# for its block of the outer relation.

	# Accesses a block of pages from an iterator.
	# This method pins pages in the buffer pool during its access.
	# We track the page ids in the block to unpin them after processing the block.
	def accessPageBlock(self, bufPool, pageIterator):
		for pid in self.pidsInBlock:
			bufPool.unpinPage(pid)
		self.pidsInBlock = list()
		M = bufPool.freeSpace()
		for i in range(0,M-2):
			try:
				(pid, page) = next(pageIterator)
			except:
				break
			#if pid is None:
			#	break
			self.pidsInBlock.append(pid)
			bufPool.getPage(pid, pinned=True)
			bufPool.pinPage(pid)

	def blockNestedLoops(self):
		riter = iter(self.rhsPlan)
		buf = self.storage.bufferPool
		while riter.hasNext():
			self.accessPageBlock(buf, riter)
			for (lPageId, lhsPage) in iter(self.lhsPlan):
				for lTuple in lhsPage:
					# Load the lhs once per inner loop.
					joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)

					for pid in self.pidsInBlock:
						rhsPage = buf.getPage(pid, pinned=True)
						for rTuple in rhsPage:
							# Load the RHS tuple fields.
							joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))

							# Evaluate the join predicate, and output if we have a match.
							if eval(self.joinExpr, globals(), joinExprEnv):
								outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
								self.emitOutputTuple(self.joinSchema.pack(outputTuple))

					# No need to track anything but the last output page when in batch mode.
					if self.outputPages:
						self.outputPages = [self.outputPages[-1]]
		self.accessPageBlock(buf, riter)
		# Return an iterator to the output relation
		return self.storage.pages(self.relationId())


	##################################
	#
	# Indexed nested loops implementation
	#
	# TODO: test
	def indexedNestedLoops(self):
		raise NotImplementedError

	##################################
	#
	# Hash join implementation.
	#
	def hashJoin(self):
		for (rPageId, rhsPage) in iter(self.rhsPlan):
			for tuple in rhsPage:
				val = self.loadSchema(self.rhsSchema, tuple)
				hash = eval(self.rhsHashFn, globals(), val) 
				self.emitOutputTupleHash(tuple, hash, False)
				
		for (lPageId, lhsPage) in iter(self.lhsPlan):
			for tuple in lhsPage:
				val = self.loadSchema(self.lhsSchema, tuple)
				hash = eval(self.lhsHashFn, globals(), val) 
				self.emitOutputTupleHash(tuple, hash, True)
		
		evalStr = ''
		for i,lt in enumerate(self.lhsKeySchema.schema()):
			rt = self.rhsKeySchema.schema()[i]
			evalStr += str(lt[0]) + '==' + str(rt[0])
			if i != 0 and i != len(self.lhsKeySchema.schema()) - 1:
				evalStr += ' and '
		if self.joinExpr is not None:
			evalStr += ' and ' + self.joinExpr
		for lk in self.outputPageHashL.keys():
			for rk in self.outputPageHashR.keys():
				
				riter = iter(self.outputPageHashR[rk])
				buf = self.storage.bufferPool
				M = buf.freeSpace() - 2
				size = len(self.outputPageHashR[rk])
				while size > 0:
					self.accessPageBlock(buf, riter)
					size -= M
					for (lPageId, lhsPage) in iter(self.outputPageHashL[lk]):
						for lTuple in lhsPage:
							# Load the lhs once per inner loop.
							joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)

							for pid in self.pidsInBlock:
								rhsPage = buf.getPage(pid, pinned=True)
								for rTuple in rhsPage:
									# Load the RHS tuple fields.
									joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))

									# Evaluate the join predicate, and output if we have a match.
									if eval(evalStr, globals(), joinExprEnv):
										outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
										self.emitOutputTuple(self.joinSchema.pack(outputTuple))

							# No need to track anything but the last output page when in batch mode.
							if self.outputPages:
								self.outputPages = [self.outputPages[-1]]
				self.accessPageBlock(buf, riter)
		# Return an iterator to the output relation
		return self.storage.pages(self.relationId())
				
	def getRelId(self, hashVal, isLeft):
		tempstr = 'temp'
		if isLeft:
			tempstr = 'templ'
		return self.relationId() + tempstr + str(hashVal)
		
	def initializeOutputHash(self, hashVal, isLeft):
		relId = self.getRelId(hashVal, isLeft)

		if self.storage.hasRelation(relId):
			self.storage.removeRelation(relId)

		if isLeft:
			self.storage.createRelation(relId, self.lhsSchema)
			self.tempFileHashL[hashVal] = self.storage.fileMgr.relationFile(relId)[1]
			self.outputPageHashL[hashVal] = []
		else:
			self.storage.createRelation(relId, self.rhsSchema)
			self.tempFileHashR[hashVal] = self.storage.fileMgr.relationFile(relId)[1]
			self.outputPageHashR[hashVal] = []

		
	def emitOutputTupleHash(self, tupleData, hashVal, isLeft):
		if isLeft:
			if hashVal not in self.tempFileHashL.keys():
				self.initializeOutputHash(hashVal, isLeft)
		else:
			if hashVal not in self.tempFileHashR.keys():
				self.initializeOutputHash(hashVal, isLeft)
			
		self.currFile = self.tempFileHashR[hashVal]
		self.currOutputPages = self.outputPageHashR[hashVal]
		if isLeft:
			self.currFile = self.tempFileHashL[hashVal]
			self.currOutputPages = self.outputPageHashL[hashVal]

		allocatePage = not(self.currOutputPages and (self.currOutputPages)[-1][1].header.hasFreeTuple())
		if allocatePage:
			# Flush the most recently updated output page, which updates the storage file's
			# free page list to ensure correct new page allocation.
			if self.currOutputPages:
				self.storage.bufferPool.flushPage((self.currOutputPages)[-1][0])
			outputPageId = self.currFile.availablePage()
			outputPage	 = self.storage.bufferPool.getPage(outputPageId)
			self.currOutputPages.append((outputPageId, outputPage))
		else:
			outputPage = (self.currOutputPages)[-1][1]

		outputPage.insertTuple(tupleData)

		if self.sampled:
			self.estimatedCardinality += 1
		else:
			self.actualCardinality += 1

	def printerr(self, string):
		f = open('err.txt', 'a')
		f.write(str(string) + '\n')
		f.close()

	# Plan and statistics information

	# Returns a single line description of the operator.
	def explain(self):
		if self.joinMethod == "nested-loops" or self.joinMethod == "block-nested-loops":
			exprs = "(expr='" + str(self.joinExpr) + "')"

		elif self.joinMethod == "indexed":
			exprs =	"(" + ','.join(filter(lambda x: x is not None, (
					[ "expr='" + str(self.joinExpr) + "'" if self.joinExpr else None ]
				+ [ "indexKeySchema=" + self.lhsKeySchema.toString() ]
				))) + ")"

		elif self.joinMethod == "hash":
			exprs = "(" + ','.join(filter(lambda x: x is not None, (
					[ "expr='" + str(self.joinExpr) + "'" if self.joinExpr else None ]
				+ [ "lhsKeySchema=" + self.lhsKeySchema.toString() ,
						"rhsKeySchema=" + self.rhsKeySchema.toString() ,
						"lhsHashFn='" + self.lhsHashFn + "'" ,
						"rhsHashFn='" + self.rhsHashFn + "'" ]
				))) + ")"

		return super().explain() + exprs
