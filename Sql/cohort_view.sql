--title :create view
CREATE  VIEW cohort_analysis AS(
WITH customer_revenue AS 
(SELECT
	s.customerkey,
	s.orderdate,
	SUM(s.quantity * s.netprice / s.exchangerate) AS net_revenue,
	COUNT(s.orderkey),
	c.countryfull ,
	c.age,
	c.givenname,
	c.surname 
FROM 
	sales s 
LEFT JOIN 
	customer c ON s.customerkey = c.customerkey 
GROUP BY 
	s.customerkey,
	s.orderdate,
	c.countryfull ,
	c.age,
	c.givenname,
	c.surname) 

SELECT 
	cr.*,
	min(cr.orderdate) OVER (PARTITION BY cr.customerkey ) AS first_purchase_date,
	EXTRACT (YEAR FROM min(cr.orderdate) OVER (PARTITION BY cr.customerkey ))::int AS cohort_year

FROM 
	customer_revenue cr
)
