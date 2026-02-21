WITH customer_ltv AS
    (SELECT
        customerkey,
        full_name,
        SUM (net_revenue) AS Life_time_value
    from 
        cohort_analysis
    GROUP BY
        customerkey,
        full_name
    ORDER BY
        customerkey),
    
    customer_segments AS 
    (SELECT 
        PERCENTILE_CONT(0.25)                              
        WITHIN GROUP (ORDER BY Life_time_value)            
        AS ltv_25th_percentile,
        PERCENTILE_CONT(0.75)                              
        WITHIN GROUP (ORDER BY Life_time_value)            
        AS ltv_75th_percentile
    
       FROM customer_ltv),
    
    segmant_value AS 
    (SELECT 
        c.* , 
        CASE 
            WHEN c.Life_time_value < ltv_25th_percentile 
            THEN 'LOW value'
            
            WHEN c.Life_time_value >= ltv_25th_percentile 
                AND c.Life_time_value <  ltv_75th_percentile 
                THEN 'MID value'
                
            ELSE 'HIGH value'
            END AS customer_segment
      FROM 
        customer_ltv c , customer_segments cs )

SELECT 
    customer_segment,
    SUM(life_time_value) AS total_ltv,
    SUM(life_time_value) / COUNT (customerkey) AS avg_ltv
FROM 
    segmant_value
GROUP BY
    customer_segment