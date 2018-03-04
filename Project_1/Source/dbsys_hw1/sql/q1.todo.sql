-- Find the top 10 parts that with the highest quantity in returned orders. 
-- An order is returned if the returnflag field on any lineitem part is the character R.
-- Output schema: (part key, part name, quantity returned)
-- Order by: by quantity returned, descending.

-- Student SQL code here:


SELECT l.partkey, p.name, SUM(l.quantity)
FROM lineitem l INNER JOIN part p ON l.partkey = p.partkey
WHERE l.returnflag = 'R'
GROUP BY l.partkey
ORDER BY SUM(l.quantity) DESC
LIMIT 10;
