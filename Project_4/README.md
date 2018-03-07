# Project 4: Query Optimization

## Description
In this project, we implement a basic query optimizer that picks an efficient plan for our query processing engine. In particular, we will implement a System-R style query optimizer that uses dynamic programming, with sampled query execution for cost estimation. This  will build on the operator algorithms and query plans used in Project 2. Recall that our query plans are tree data structures, comprised of operator nodes. Our query optimizer will consider alternate tree plans that are guaranteed to produce the same query results, picking the lowest cost plan to execute and answer the query. These alternate plans are derived by considering algebraic equivalences for the relational algebra. Specifically in this assignment, you will consider plan rewrites that reorder joins, and push down selection and project operations. 

Our support code for this assignment provides the definition and API for the query optimizer class, as well as a working implementation of the query processor you saw in Project 2. Furthermore, we have extended this query processor to support sampling (completed for you), and cost computation (API provided for you to fill in). You will continue to construct query plans using our PlanBuilder class. This plan must be optimized with the query optimizer prior to query execution. Given a query plan as its input, the query optimizer will construct a new plan for actual use during execution. 

At the end of this project, we have a declarative database engine that introspects on the queries to improve system efficiency and scalability. 

## Code File
<dl>
 <dd><pre>
	Source/               ## Source code directory
	  |-- Database.py          ## Top-level database API
	  |-- Catalog/             ## Catalog as in HW1 and 2
	  |-- Storage/             ## Storage engine as in HW1 and 2
	  |-- Query/
	  |---- Plan.py            ## Query plans, NEW: sampling API
	  |---- Operator.py        ## Operator base class, NEW: costing API
	  |---- Optimizer.py       ## NEW: optimizer class skeleton
	  |---- Operators/         ## Operator implementations directory
	  |------ TableScan.py     ## Scan operator, using file-based iterators
	  |------ *.py             ## Select, Project, Union, Join, and Group-By operator implementations.
 </pre></dd> 
</dl>	  

### [Project 4 Source](http://damsl.cs.jhu.edu/teaching/dbsys/2017/assignments/hw3/)
