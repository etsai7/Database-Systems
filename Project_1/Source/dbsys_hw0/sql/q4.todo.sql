-- For each of the top 5 nations with the greatest value (i.e., total price) of orders placed,
-- find the top 5 nations which supply these orders.
-- Output schema: (Order placer name, Order supplier name, value of orders placed)
-- Order by: (Order placer, Order supplier)

-- Notes
--  1) We are expecting exactly 25 results 

-- Student SQL code here:

WITH totsum(natk, custk, qtp, natn) AS (
SELECT n.nationkey, c.custkey, SUM(o.totalprice), n.name
FROM nation n JOIN customer c ON n.nationkey = c.nationkey
JOIN orders o ON c.custkey = o.custkey
GROUP BY  n.nationkey
ORDER BY SUM(o.totalprice) DESC
LIMIT 5),

supp(ordn, val, suppn) AS ( 
SELECT n.nationkey, SUM(o.totalprice), s.suppkey
FROM nation n, totsum t JOIN customer c ON n.nationkey = c.nationkey
JOIN orders o ON c.custkey = o.custkey
JOIN lineitem l ON o.orderkey = l.orderkey
JOIN supplier s ON s.suppkey = l.suppkey
WHERE t.natk = n.nationkey 
GROUP BY n.nationkey, s.nationkey
ORDER BY SUM(o.totalprice) DESC
),

temp(n1, n2, val) AS ( 
SELECT n.name, n2.name, s1.val FROM supp s1
JOIN nation n ON s1.ordn = n.nationkey
JOIN supplier su ON s1.suppn = su.suppkey
JOIN nation n2 ON su.nationkey = n2.nationkey
	WHERE s1.suppn IN ( 
        SELECT s2.suppn FROM supp s2
        WHERE s2.ordn = s1.ordn
        ORDER BY s2.val DESC
        LIMIT 5
    )
GROUP BY s1.ordn, s1.suppn
ORDER BY n2.name ASC
)

SELECT * FROM temp
--GROUP BY temp.n1
ORDER BY temp.n1 ASC

--SELECT n.name, s.suppn, s.val
--FROM totsum t, temp s JOIN nation n ON t.natk = n.nationkey
----WHERE t.natk = s.ordn 
--GROUP BY t.natk
--ORDER BY n.name, s.val DESC
;
