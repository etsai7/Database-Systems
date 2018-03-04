import Database
from Catalog.Schema import DBSchema

import sys

import warnings


warnings.simplefilter("ignore", ResourceWarning)
db = Database.Database()

qpart2 = db.query().fromTable('part').join(\
             db.query().fromTable('partsupp'), \
             method='nested-loops', \
             expr='p_partkey == ps_partkey').join(\
             db.query().fromTable('supplier'), \
             method='nested-loops', \
             expr='ps_suppkey == s_suppkey').where('ps_supplycost < 5').select({ 	'p_name'   : ('p_name', 'text'), 
						's_name' : ('s_name', 'text') })

db.query().fromTable('part').join(\
             db.query().fromTable('partsupp'), \
             method='nested-loops', \
             expr='p_partkey == ps_partkey').join(\
             db.query().fromTable('supplier'), \
             method='nested-loops', \
             expr='ps_suppkey == s_suppkey').where('ps_availqty == 1').select({ 'p_name'   : ('p_name', 'text'), 
						's_name' : ('s_name', 'text') }).union(qpart2).finalize()
