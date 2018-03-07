# Project 4: Query Optimization

## Description
In this project, we implement a basic query optimizer that picks an efficient plan for our query processing engine. In particular, we will implement a System-R style query optimizer that uses dynamic programming, with sampled query execution for cost estimation. This  will build on the operator algorithms and query plans used in Project 2. Recall that our query plans are tree data structures, comprised of operator nodes. Our query optimizer will consider alternate tree plans that are guaranteed to produce the same query results, picking the lowest cost plan to execute and answer the query. These alternate plans are derived by considering algebraic equivalences for the relational algebra. Specifically in this assignment, you will consider plan rewrites that reorder joins, and push down selection and project operations. 

Our support code for this assignment provides the definition and API for the query optimizer class, as well as a working implementation of the query processor you saw in Project 2. Furthermore, we have extended this query processor to support sampling (completed for you), and cost computation (API provided for you to fill in). You will continue to construct query plans using our PlanBuilder class. This plan must be optimized with the query optimizer prior to query execution. Given a query plan as its input, the query optimizer will construct a new plan for actual use during execution. 

At the end of this project, we have a declarative database engine that introspects on the queries to improve system efficiency and scalability. 

* [Part 1](#part1): Push Down Optimization
* [Part 2](#part2): Join Order Optimization
* [Part 3](#part3): Cost Model Design
* [Part 4](#part4): Bushy Plans and Optimizer Scalability

<a name="part1"></a>
### Part 1: Push Down Optimization

The `Optimizer.pushdownOperator` method rearranges the unary operators (selection, projection) so that they occur as close to their respective base relations as possible. It also ensures that the attributes referred to in join predicates but are not explicitly requested by the query are not projected out before the join actually occurs. 
The following assumptions were made  when implementing push down optimization:

* Attribute names are unique, and appear in only one relation in a plan.
* All 'Select' expressions are in Conjunctive Normal Form
* Operators do not need to be pushed down 'through' a GroupBy Expression

<a name="part2"></a>
### Part 2: Join Order Optimization

The main join-selection algorithm implemented in this project is a System-R-style dynamic programming algorithm. We only consider left-deep plans for this particular project, a subsequent part will ask you to additionally consider bushy plans. To recap, the System-R optimization algorithm builds up a table of the best join orderings and join methods by cost, successively for each sized subset of relations in a query. That is, it first determines the best access methods for each relation, the best join ordering and algorithm for each pair of relations using the access methods previously computed, and so on.

This algorithm consists of two steps during each pass -- an enumeration of viable candidate plans for the given subsets of relations, and an evaluation of the best plan in each subset. For a left-deep-only optimizer, you only need to consider plans where the right-hand-side operand to the join is a base relation, with potential unary operators attached.

The determination of the best plan for each subset of relations should be done by evaluating the plan over a sampled portion of the dataset, and estimating the cost of that plan. The runtime cost of the optimizer can be mitigated by caching cost estimations across queries over the same tables.

The complexity of the optimizer depends in large part on the assumptions we can make about the plan given to the optimizer. For the purposes of this project, we made the following assumptions:

* Joins are only ever between two relations, and join expressions only refer to attributes from the two relations being joined.

<a name="part3"></a>
### Part 3: Cost Model Design

The default cost model for an operator uses the following formula:

``` cost = total inputs from children * per-tuple cost ```

This is not a very accurate cost formula for join operations, and does not distinguish the various join algorithms (nested loops, block nested loops or hash join) present in the codebase. This is similarly true for the group-by operator. In this project, we overload the Operator.cost method for the join and group-by operations, to provide a more suitable cost model. Our cost model uses existing statistics such as local and child selectivities and cardinalities. For example, for the hash-join algorithm, we incorporate properties such as the hash table build phase amongst other algorithm phases. (This is an additive component of the cost rather than a multiplicative component.) We also collect other statistics during sampling or query execution and use them as part of the cost model. 

<a name="part4"></a>
### Part 4: Bushy Plans and Optimizer Scalability

In this part, we extend the join ordering optimization implemented in [Part 2](#part2) in two ways:

* In a `BushyOptimizer` class that inherits from `Optimizer`, we implement a variant of the dynamic programming optimizer that considers bushy plans as well as left-deep plans.
* In a `GreedyOptimizer` class that inherits from `Optimizer`, we implement a **greedy** optimizer variant that greedily constructs plans using the cheapest join available over the subplans as described in our lecture slides.

With these two variants, we experimentally evalute the optimizer's scalablity reporting the number of plans considered (kept as a counter in your algorithm implementation), and the optimizer's running time. We apply these to join plans that are of size 2, 4, 6, 8, 10, 12. For this experiment, we use a synthetic database schema, consisting of relations with triples of integers (e.g., R(a: int, b: int, c: int), S(d: int, e: int, f: int) and so forth). 

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
