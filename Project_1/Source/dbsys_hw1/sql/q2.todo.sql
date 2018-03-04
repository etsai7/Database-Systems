--  Find the 10 customers who spent the highest average number of days waiting for shipments.
-- A customer is waiting between a shipment's ship date and receipt date
-- Output schema: (customer key, customer name, average wait)
-- Order by: average wait DESC

-- Notes
--  1) Use the sqlite DATE(<text>) function to interpret a text field as a date.
--  2) Use subtraction to compute the duration between two dates (e.g., DATE(column1) - DATE(column2)).
--  3) Assume that a package cannot be received before it is shipped.

-- Student SQL code here:


SELECT c.custkey, c.name, AVG(DATE(l.receiptdate) - DATE(l.shipdate)) 
FROM customer c INNER JOIN orders o ON c.custkey = o.custkey
JOIN lineitem l ON o.orderkey = l.orderkey
GROUP BY c.custkey
ORDER BY AVG(DATE(l.receiptdate) - DATE(l.shipdate)) DESC
LIMIT 10;
