-- Find the name of the most heavily ordered (i.e., highest quantity) part per nation.
-- Output schema: (nation key, nation name, part key, part name, quantity ordered)
-- Order by: (nation key, part key) SC

-- Notes
--   1) You may use a SQL 'WITH' clause for common table expressions.
--   2) A single nation may have more than 1 most-heavily-ordered-part.

-- Student SQL code here:
WITH sub (nk, nam, pk, pn, q) AS (
	SELECT n.nationkey, n.name, l.partkey, p.name, SUM(l.quantity)
	FROM nation n JOIN customer c ON n.nationkey = c.nationkey
	JOIN orders o ON c.custkey = o.custkey
	JOIN lineitem l ON o.orderkey = l.orderkey
	JOIN part p ON l.partkey = p.partkey
	GROUP BY n.nationkey, l.partkey
	ORDER BY n.nationkey ASC
),
maxq (nk, q) AS (
	SELECT sub.nk, MAX(sub.q)
	FROM sub
	GROUP BY sub.nk
)
SELECT s1.nk, s1.nam, s1.pk, s1.pn, s1.q
FROM sub s1 JOIN maxq ON maxq.nk = s1.nk
WHERE s1.q = maxq.q
--GROUP BY s1.nk
ORDER BY s1.nk, s1.pk DESC
;
--)

