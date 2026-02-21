SELECT 
	cohort_year,
	COUNT (DISTINCT customerkey) AS total_customers,
	SUM(net_revenue):: INT  AS total_revenue,
	(SUM(net_revenue) / COUNT (DISTINCT customerkey))::int  AS customer_revenue
FROM 
	cohort_analysis
WHERE
	orderdate = first_purchase_date
GROUP BY 
	cohort_year
ORDER BY 
	cohort_year

