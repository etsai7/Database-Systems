# Project 2: Storage Engine

## Description
A database storage engine provides an abstraction layer for the memory-disk storage boundary. With this abstraction, database queries can use iterators to traverse all of the records in a relation, and subsequently process these records. The storage engine provides efficient block-based access to the disk, as well as caching of blocks during iteration over a relation's tuples. 

This project designs and implement three components of a database storage engine, namely a contiguous and slotted page layout scheme, a heap file external data structure to store relations, and a buffer pool to cache pages as they are accessed from disk-resident heap files. Together, these components will let you load and store relational data in a binary format, access them in page-oriented fashion, and provide basic caching of "hot" pages in main-memory. The storage engine that you implement here will hold the datasets used throughout the course, and acts as an input to the query processing layer that we will see in the second part. 

Specifically, the project includes:

* Implementing two page layout schemes to store multiple records.
* Implementing a heap file data structure to hold all of the pages (and records) comprising a relation.
* Implementing a buffer pool, as a cache for pages brought into memory from heap files.
* Conducting experiments on the storage performance of your components.

* [Part 1](#part1): Contiguous and Slotted Pages
* [Part 2](#part2): Heap Files
* [Part 3](#part3): Buffer Pool

<a name="part1"></a>
### Part 1: Contiguous and Slotted Pages
Pages are byte arrays that provide the storage for the binary representation of tuples created with a `DBSchema` object. We ask you to implement both a contiguous page, and a slotted page. This will require implementation of the following classes:

* `Storage.Page.Page`
* `Storage.Page.PageHeader`
* `Storage.SlottedPage.SlottedPage`
* `Storage.SlottedPage.SlottedPageHeader`

The contiguous page and its header should be implemented in the **Storage/Page.py** file, and the slotted page in the **Storage/SlottedPage.py** file. Both types of page are implemented as a fixed size, in-memory, mutable byte array through Python's [BytesIO](http://docs.python.org/3/library/io.html#io.BytesIO) objects. By inheriting from `BytesIO`, we can modify the page at arbitrary offsets, and at arbitrary subranges with Python's slice operations, for example to insert or access packed tuples. In particular, we can use a BytesIO's `getbuffer()` method to obtain a [memoryview](http://docs.python.org/3.3/library/stdtypes.html#memoryview) object, through which we can perform simple byte-level operations. See the the example using the `getbuffer()` method [here](http://docs.python.org/3/library/io.html#io.BytesIO) 

<a name="part2"></a>
### Part 2: Heap Files
<a name="part3"></a>
### Part 3: Buffer Pool

## Code File
<dl>
 <dd><pre>
 dbsys-hw2/             ## Source code directory
  |-- Catalog            ## Catalog utilities (i.e., schemas and types)
  |---- Identifiers.py   ## Identifier types, to distinguish tuples, pages and files.
  |---- Schema.py        ## Schema implementation
  |
  |-- Storage            ## DB storage layer implementation
  |---- BufferPool.py    ## Buffer pool skeleton
  |---- File.py          ## Heap file components
  |---- FileManager.py   ## File manager, tracks which files implement which relations.
  |---- Page.py          ## Contiguous page
  |---- SlottedPage.py   ## Slotted page
  |---- StorageEngine.py ## Storage engine API for other DBMS components
  |
  |-- Tests              ## Testing 
  |---- unit.py          ## Test cases for students 
 </pre></dd> 
</dl>
