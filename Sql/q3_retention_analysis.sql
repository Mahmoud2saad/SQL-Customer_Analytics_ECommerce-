WITH customer_indexing AS
	(SELECT 
		row_number()OVER (PARTITION BY customerkey ORDER BY orderdate DESC) AS customer_operation_index,
		customerkey,
		orderdate,
		full_name,
		first_purchase_date,
		cohort_year
	FROM 
		cohort_analysis),
		
churned_customer AS 

	(SELECT 
		customerkey,
		full_name,
		first_purchase_date,
		orderdate AS last_purchase_date,
		cohort_year,
		CASE
			WHEN orderdate < (SELECT MAX (orderdate) FROM SALES) - INTERVAL '6 months'  THEN  'Churned'
			ELSE 'Active'
		END AS customer_status
		
	FROM 
		customer_indexing
	WHERE 
		customer_operation_index = 1 AND 
		first_purchase_date < (SELECT MAX (orderdate) FROM SALES) - INTERVAL'6 months')
		
SELECT  
	cohort_year,
	customer_status,
	COUNT(customer_status) AS num_customers,
	COUNT(customer_status) / SUM (COUNT(customer_status)) OVER(PARTITION BY cohort_year)
	AS customer_status_pct
FROM 
	churned_customer
GROUP BY 
	cohort_year,
	customer_status
	
