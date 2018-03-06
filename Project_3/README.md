# Project 3: Query Processing

## Description
Building on our storage engine, the third project will cover the query operator algorithms used to efficiently process SQL queries over out-of-core datasets. These algorithms provide a physical layer implementation of the relational algebra operations that comprise query plans. Our query plans are represented as tree data structures, with each operator in the tree supporting a common abstraction in the form of an iterator for each input. In this way, a query plan is processed starting at its root operator and its request for the first output tuple from its output iterator. This recursively requests the next tuple from each of operator iterators in the plan tree, with query processing terminating when all of these iterators are complete. We use a set-at-a-time processing techniques, where each operator will output its intermediate results in a temporary file in the storage engine. 

Thie project includes the mplementation of three basic operators: table scans, selects and projects which will help with simple queries such as `select id, age from employees where salary < 50000`. Further implementations extend the set of primitive operators, providing the implementation of unions, group-by aggregations, and a variety of join algorithms (block nested loops, indexed, and hash joins). We will be using Berkeley DB to provide unclustered index data structures in this assignment, where these indexes will contain tuple identifiers to reference data in our storage engine. 

This project is a working single-threaded database engine that supports data loading and storage, and data processing for a subset of SQL-style queries. We will then use this engine to study further topics such as query optimization and distributed data management. 

* [Part 1](#part1): Union and Block nested loops join
* [Part 2](#part2): Group-by Aggregation
* [Part 3](#part3): Hash join
* [Part 4](#part4): External Sort

<a name="part1"></a>
### Part 1: Union and Block nested loops join

Implemented the operators for union and block-nested-loops join. Union is a good place to start adding to the codebase, because it's relatively simple. Pay attention to the fact that union can work either in page-at-a-time or set-at-a-time mode, that is you will need to implement both of its `processInputPage` and `processAllPages` method. 

Block-nested-loop join is a more efficient variant of the simple nested-loop join, whereby the outer relation is fully loaded into all available memory in the buffer pool. In order to keep block pages from being evicted from the buffer pool, the engine uses the page-pinning interface which has been introduced in this version of the codebase.

<a name="part2"></a>
### Part 2: Group-by Aggregation

Implemented the group-by aggregation operator. This operator partitions the records into groups and computes an aggregation per group, for all tuples residing in the group. The grouping values are determined by groupExpr, while the number of partitions is determined by groupHashFn. Each tuple is fed to the groupExpr and groupHashFn, and then added to the resulting partition using the aggregation function. 

The engine supports the possibility of needing to operate on a large number of groups exceeding the size of main memory. Thus for each partition indiciated by the groupHashFn function, a temporary partition file is created using the storage engine available in the operator.

<a name="part3"></a>
### Part 3: Hash join

Implemented the hash join operator through the `Query.Operators.Join`hashJoin method. The hash join algorithm involves using a hashing function to partition both relations into small partitions that fit in memory. A temporary partition file for each partition is created, for both relations. Pairs of in-memory partitions can then be processed with a block-nested loop join to find matching tuples. We have provided the parameters passed in to the hash join in the section on "Creating query plans" above. Please note that while the key schema paramaters define an equality predicate (where fields from `lhsKeySchema` must be equal to the fields from `rhsKeySchema`), the join expression may contain additional predicates (i.e., range predicates, etc) that must be evaluated as part of the matching

<a name="part4"></a>
### Part 4: External Sort

Implemented an external sort operator. This operator should process the input in multiple passes, with the first pass create partitions that fit in main memory, along with performing an in-place sort in main-memory per partition. It fills up available memory with as much as can fit from an input operator (using the pinning interface which is new in this codebase), and then save the pages back to disk. We use python's built-in in-place sort routine. This will create multiple sorted runs. Later phases must then merge the multiple runs by iterating simultaneously over 2 runs and producing a merged output in the desired sort order.

## Code File
<dl>
 <dd><pre>
	Source/                    ## Source code directory
	  |-- Database.py          ## Top-level database API
	  |-- Catalog/             ## Catalog as in HW1
	  |-- Storage/             ## Storage engine as in HW1
	  |---- BufferPool.py      ## New in this codebase: BufferPool with pinning
	  |---- Index/
	  |------ IndexManager.py  ## New in this codebase: indexes for joins.
	  |-- Query/
	  |---- Plan.py            ## Query plan data structure
	  |---- Operator.py        ## Abstract base class for operator implementations
	  |---- Operators/         ## Operator implementations directory
	  |------ TableScan.py     ## Scan operator, using file-based iterators
	  |------ Select.py        ## Filters the input tuples based on a predicate
	  |------ Project.py       ## Projects attributes, without eliminating duplicates
	  |------ Union.py         ## Unions two multisets, which must have the same schema
	  |------ Join.py          ## Four joins: nested, block nested, index, hash
	  |------ GroupBy.py       ## Group by aggregation
	  |------ Sort.py          ## External sorting
	  |-- Tests/
	  |---- hw2.py             ## HW2 Test Cases
 </pre></dd> 
</dl>

### [Project 3 Source](http://damsl.cs.jhu.edu/teaching/dbsys/2017/assignments/hw2/)

