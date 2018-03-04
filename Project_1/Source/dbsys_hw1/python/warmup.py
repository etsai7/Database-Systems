import struct
import sys

# Construct objects w/ fields corresponding to columns.
# Store fields using the appropriate representation:
	# TEXT => bytes
	# DATE => bytes
	# INTEGER => int
	# FLOAT => float

class Lineitem(object):
	# The format string, for use with the struct module.
	fmt = 'I I I I f f f f 1s 1s 10s 10s 10s 25s 10s 44s'
	l_orderkey       = 0
	l_partkey        = 0
	l_suppkey        = 0
	l_linenumber     = 0
	l_quantity       = 0
	l_extendedprice  = 0
	l_discount       = 0
	l_tax            = 0
	l_returnflag     = 0
	l_linestatus     = 0
	l_shipdate       = 0
	l_commitdate     = 0
	l_receiptdate    = 0
	l_shipinstruct   = 0
	l_shipmode       = 0
	l_comment        = 0
	# Initialize a lineitem object.
	# Arguments are strings that correspond to the columns of the tuple.
	# Feel free to use __new__ instead.
	# (e.g., if you decide to inherit from an immutable class).
	def __init__(self, *args):
		self.l_orderkey
		self.l_partkey
		self.l_suppkey
		self.l_linenumber
		self.l_quantity
		self.l_extendedprice
		self.l_discount
		self.l_tax
		self.l_returnflag
		self.l_linestatus
		self.l_shipdate
		self.l_commitdate
		self.l_receiptdate
		self.l_shipinstruct
		self.l_shipmode
		self.l_comment
		for i,e in enumerate(args):
			if i % 16 == 0:
				self.l_orderkey = int(float(e))
			elif i % 16 == 1:
				self.l_partkey = int(float(e))
			elif i % 16 == 2:
				self.l_suppkey = int(float(e))
			elif i % 16 == 3:
				self.l_linenumber = int(float(e))
			elif i % 16 == 4:
				self.l_quantity = float(e)
			elif i % 16 == 5:
				self.l_extendedprice = float(e)
			elif i % 16 == 6:
				self.l_discount = float(e)
			elif i % 16 == 7:
				self.l_tax = float(e)
			elif i % 16 == 8:
				self.l_returnflag = e
			elif i % 16 == 9:
				self.l_linestatus = e
			elif i % 16 == 10:
				self.l_shipdate = e
			elif i % 16 == 11:
				self.l_commitdate = e
			elif i % 16 == 12:
				self.l_receiptdate = e
			elif i % 16 == 13:
				self.l_shipinstruct = e
			elif i % 16 == 14:
				self.l_shipmode = e
			elif i % 16 == 15:
				self.l_comment = e
	# Pack this lineitem object into a bytes object.
	def pack(self):
	#'5i3fss10s10s10s25s10s44s'
		package = struct.pack(self.fmt, 
		    self.l_orderkey,
		    self.l_partkey,
		    self.l_suppkey,
		    self.l_linenumber,
		    self.l_quantity,
		    self.l_extendedprice,
		    self.l_discount,
		    self.l_tax,
		    self.l_returnflag.ljust(1).encode('ASCII'),
		    self.l_linestatus.ljust(1).encode('ASCII'),
		    self.l_shipdate.ljust(10).encode('ASCII'),
		    self.l_commitdate.ljust(10).encode('ASCII'),
		    self.l_receiptdate.ljust(10).encode('ASCII'),
		    self.l_shipinstruct.ljust(25).encode('ASCII'),
		    self.l_shipmode.ljust(10).encode('ASCII'),
		    self.l_comment.ljust(44).encode('ASCII'))
		#print 'l: ' + str(struct.calcsize(package))
		return package
	# Construct a lineitem object from a bytes object.
	@classmethod
	def unpack(cls, byts):
		data = struct.unpack('I I I I f f f f 1s 1s 10s 10s 10s 25s 10s 44s', byts)
		for o in data:
			if type(o) is str:
				o = str.strip(o)
		return cls(*data)

	# Return the size of the packed representation.
	# Do not change.
	@classmethod
	def byteSize(cls):
		return struct.calcsize(cls.fmt)

		
class Orders(object):
	# The format string, for use with the struct module.
	fmt = "I I 1s f 10s 15s 15s I 79s"
	o_orderkey       = 0
	o_custkey        = 0
	o_orderstatus    = 0
	o_totalprice     = 0
	o_orderdate      = 0
	o_orderpriority  = 0
	o_clerk          = 0
	o_shippriority   = 0
	o_comment        = 0
	# Initialize an orders object.
	# Arguments are strings that correspond to the columns of the tuple.
	# Feel free to use __new__ instead.
	# (e.g., if you decide to inherit from an immutable class).
	def __init__(self, *args):
		self.o_orderkey
		self.o_custkey
		self.o_orderstatus
		self.o_totalprice
		self.o_orderdate
		self.o_orderpriority
		self.o_clerk
		self.o_shippriority
		self.o_comment
		for i,e in enumerate(args):
			if i % 9 == 0:
				self.o_orderkey = int(float(e))
			elif i % 9 == 1:
				self.o_custkey = int(float(e))
			elif i % 9 == 2:
				self.o_orderstatus = e
			elif i % 9 == 3:
				self.o_totalprice = float(e)
			elif i % 9 == 4:
				self.o_orderdate = e
			elif i % 9 == 5:
				self.o_orderpriority = e
			elif i % 9 == 6:
				self.o_clerk = e
			elif i % 9 == 7:
				self.o_shippriority = int(float(e))
			elif i % 9 == 8:
				self.o_comment = e
			
	# Pack this orders object into a bytes object.
	def pack(self):
	#'2i1sf10s15s15si79s'
		package = struct.pack(self.fmt, 
			self.o_orderkey,
		    self.o_custkey,
		    self.o_orderstatus.ljust(1).encode('ASCII'),
		    self.o_totalprice,
		    self.o_orderdate.ljust(10).encode('ASCII'),
		    self.o_orderpriority.ljust(15).encode('ASCII'),
		    self.o_clerk.ljust(15).encode('ASCII'),
		    self.o_shippriority,
		    self.o_comment.ljust(79).encode('ASCII'))
		#print 'o: ' + str(struct.calcsize(package))
		return package

	# Construct an orders object from a bytes object.
	@classmethod
	def unpack(cls, byts):
		data = struct.unpack("I I 1s f 10s 15s 15s I 79s", byts)
		for o in data:
			if type(o) is str:
				o = str.strip(o)
		return cls(*data)
	
	# Return the size of the packed representation.
	# Do not change.
	@classmethod
	def byteSize(cls):
		return struct.calcsize(cls.fmt)

# Return a list of 'cls' objects.
# Assuming 'cls' can be constructed from the raw string fields.
def readCsvFile(inPath, cls, delim='|'):
	lst = []
	with open(inPath, 'r') as f:
		for line in f:
			fields = line.strip().split(delim)
			lst.append(cls(*fields))
	return lst

# Write the list of objects to the file in packed form.
# Each object provides a 'pack' method for conversion to bytes.
def writeBinaryFile(outPath, lst):
	newfile = open(outPath, "wb")
	for obj in lst:
		newfile.write(obj.pack())

# Read the binary file, and return a list of 'cls' objects.
# 'cls' provicdes 'byteSize' and 'unpack' methods for reading and conversion.
def readBinaryFile(inPath, cls):
	f = open(inPath, "rb")
	result = f.read(cls.byteSize())
	l = list()
	while result != '':
		l.append(cls.unpack(result))
		result = f.read(cls.byteSize())
	return l