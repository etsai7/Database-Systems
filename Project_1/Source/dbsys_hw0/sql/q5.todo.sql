-- Find the customer market segments where the yearly total number of orders declines 
-- in the last 2 years in the dataset. Note the database will have different date 
-- ranges per market segment, for example segment A records between 1990-1995, and 
-- segment B between 1992-1998. That is for segment A, we want the difference between 
-- 1995 and 1994.
-- Output schema: (market segment, last year for segment, difference in # orders)
-- Order by: market segment ASC 

-- Notes
--  1) Use the sqlite function strftime('%Y', <text>) to extract the year from a text field representing a date.
--  2) Use CAST(<text> as INTEGER) to convert text (e.g., a year) into an INTEGER.
--  3) You may use a SQL 'WITH' clause.

-- Student SQL code here:


WITH orderdata (segment, yr, ordercount) AS (
	SELECT cust.mktsegment, CAST(STRFTIME('%Y', o.orderdate) as INTEGER), COUNT(o.orderkey)
	FROM customer cust JOIN orders o ON cust.custkey = o.custkey
	GROUP BY cust.mktsegment, STRFTIME('%Y', o.orderdate)
	--ORDER BY CAST(STRFTIME('%Y', o.orderdate) as INTEGER) DESC
)
SELECT od1.segment, MAX(od1.yr), od1.ordercount - od2.ordercount
FROM orderdata od1 JOIN orderdata od2 ON (od1.yr - 1) = od2.yr
WHERE od1.segment = od2.segment AND od1.ordercount < od2.ordercount
GROUP BY od1.segment
HAVING MAX(od1.yr)
ORDER BY od1.segment ASC
;
--yearmax (segment, yrmx) AS (
--	SELECT cust.mktsegment, MAX(STRFTIME('%Y', o.orderdate))
--	FROM customer cust JOIN orders o ON cust.custkey = o.custkey
--	GROUP BY cust.mktsegment, STRFTIME('%Y', o.orderdate)
--),
--year2max (segment, yr2mx) AS (
--	SELECT cust.mktsegment, MAX(STRFTIME('%Y', o.orderdate))
--	FROM customer cust JOIN orders o ON cust.custkey = o.custkey
--	JOIN yearmax ON yearmax.segment = cust.mktsegment
--	WHERE STRFTIME('%Y', o.orderdate) != yearmax.yrmx
--	GROUP BY cust.mktsegment, STRFTIME('%Y', o.orderdate)
--	
--),
--maxyear (segment, yr, orders) AS (
--	SELECT od.segment, temp.yr, od.orders
--	FROM orderdata od JOIN yearmax ym ON od.segment = yearmax.segment AND yearmax.yrmx = od.yr 
--	WHERE yearmax.yrmx = od.yr 
--	GROUP BY od.segment 
--	
--),
--max2year (segment, yr, orders) AS (
--	SELECT od.segment, temp.yr, od.orders 
--	FROM orderdata od JOIN year2max y2m JOIN od.segment = y2m.yrmx AND y2m.yr2mx = od.yr 
--	WHERE y2m.yrmx = od.yr
--	GROUP BY od.segment
--	
--)
--SELECT my.segment, my.yr, m2y.orders - my.orders
--FROM maxyear my JOIN max2year m2y ON my.segment = m2y.segment
--GROUP BY my.segment
--ORDER BY my.segment ASC
--WHERE m2y.orders - my.orders > 0;