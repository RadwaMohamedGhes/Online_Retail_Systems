>>> Question 1

FIRST QUERY:

-- ranking total sales from every customer 
SELECT customer_id,  SUM(quantity * price) total_sales, RANK() OVER(ORDER BY SUM(quantity * price) DESC) rank_sales
FROM tableretail
GROUP BY customer_id;
-------------------------------------------------------------------------------------------------------------------------

SECOND QUERY:

-- extract month from the invoice date
SELECT TO_CHAR(TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi'), 'mm') month,
-- calculating monthly sales and running total sales according to month for our top customer
            SUM(quantity * price) monthly_sales,
            SUM(SUM(quantity * price)) 
            OVER (ORDER BY TO_CHAR(TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi'), 'mm'))
            running_total   

FROM tableretail
-- filter data based on our top customer
WHERE customer_id = 12931
GROUP BY TO_CHAR(TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi'), 'mm')
ORDER BY month;
--------------------------------------------------------------------------------------------------------------------------

THIRD QUERY:

-- calculate monthly sales and  monthly sales growth 
SELECT TO_CHAR(TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi'), 'mm') month, SUM(quantity * price) monthly_sales,
       -- calculate pervious month sales using lag function 
       LAG(SUM(quantity * price)) OVER (ORDER BY TO_CHAR(TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi'), 'mm')) previous_month_sales,
       -- monthly sales growth = total sales of current month - total sales of previous month 
       SUM(quantity * price) - LAG(SUM(quantity * price)) OVER (ORDER BY TO_CHAR(TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi'), 'mm')) monthly_sales_growth
FROM tableretail
GROUP BY TO_CHAR(TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi'), 'mm')
ORDER BY month;
----------------------------------------------------------------------------------------------------------------------------

FOURTH QUERY:

WITH tenure_vs_sales AS (
SELECT DISTINCT customer_id, Sum(quantity * price) OVER(PARTITION BY customer_id) total_sales,

-- first purchase every customer made
FIRST_VALUE(TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi')) OVER(PARTITION BY customer_id ORDER BY TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi') ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) first_purchase,

-- last purchase every customer made
LAST_VALUE(TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi')) OVER(PARTITION BY customer_id ORDER BY TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi') ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_purchase,

-- the tenure that every customer made his purchases on
ROUND(LAST_VALUE(TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi')) OVER(PARTITION BY customer_id ORDER BY TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi') ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) -
FIRST_VALUE(TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi')) OVER(PARTITION BY customer_id ORDER BY TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi') ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) tenure
FROM tableretail
)
-- correlation between the tenure that every customer made his purchases on and total sales
SELECT tenure_vs_sales.*, CORR(total_sales, tenure) OVER() corr_tenure_sales
FROM tenure_vs_sales;
------------------------------------------------------------------------------------------------------------------------------

FIFTH QUERY:

-- finding out the top 10 sold products 
WITH most_sold_products AS (
SELECT stockcode, SUM(quantity) number_of_orders
FROM tableretail
GROUP BY stockcode
ORDER BY number_of_orders DESC
)
SELECT *
FROM  most_sold_products 
WHERE ROWNUM <= 10; 


-------------------------------------------------------------------------------------------------------------------------------

>>> QUESTION 2

-- this query for calculating recency: diff between the last purchase overall and the last purchase by the customer
-- frequency: count of orders the customer has bought from our store 
-- monetary: total purchases that every customer paid for products
WITH recency_frequency_monetary AS (
SELECT DISTINCT customer_id, ROUND(FIRST_VALUE(TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi')) OVER(ORDER BY TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi') DESC) 
            - FIRST_VALUE(TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi')) OVER(PARTITION BY customer_id ORDER BY TO_DATE(invoicedate, 'mm/dd/yyyy hh24:mi') DESC)) recency,
            COUNT(*) OVER(PARTITION BY customer_id) frequency, 
            SUM(quantity * price) OVER (PARTITION BY customer_id) monetary
FROM tableretail
),
-- categorizing recency, frequency, and monetary into 5 categories(scores)
scores AS (
SELECT recency_frequency_monetary.*, NTILE(5) OVER(ORDER BY recency DESC) r_score,
            NTILE(5) OVER(ORDER BY(frequency + monetary) / 2) fm_score
FROM recency_frequency_monetary
)
-- label our customers according to their scores
SELECT scores.*, CASE WHEN (r_score = 5 AND fm_score in (5,4)) OR (r_score = 4 AND fm_score = 5) then 'Champions' 
                                   WHEN (r_score in (4,5) AND fm_score = 2) OR (r_score in (4,3) AND fm_score    = 3) then 'Potential Loyalists'
                                   WHEN (r_score = 5 AND fm_score = 3) OR (r_score = 4 AND fm_score = 4) OR (r_score = 3 AND fm_score in (5,4)) then 'Loyal Customers'
                                   WHEN (r_score = 5 AND fm_score = 1) then 'Recent Customers'
                                   WHEN (r_score in (4,3) AND fm_score = 1) then 'Promising'
                                   WHEN (r_score = 3 AND fm_score = 2) OR (r_score = 2 AND fm_score in (2,3)) then 'Customers Needing Attention'
                                   WHEN (r_score = 1 AND fm_score = 3) OR (r_score = 2 AND fm_score in (4,5)) then 'At Risk'
                                   WHEN  (r_score = 1 AND fm_score in (4,5)) then 'Cant Lose Them'                    
                                   WHEN (r_score = 1 AND fm_score = 2) then 'Hibernating'
                                   WHEN (r_score = 1 AND fm_score = 1) then 'Lost'
                          END AS cust_segment
FROM scores;

NOTE: I did not use ELSE because there is another probability that r_score = 2 and fm_score = 1 which do not exist in the table that we had to make the cust_segment column from it 
so I did not make a segment for it and did not use ELSE because it will assign this probability to Lost.
--------------------------------------------------------------------------------------------------------------------------------

>>> QUESTION 3

QUERY 1:

 -- assign a unique row number to each transaction for each customer
WITH unique_transaction AS (
SELECT cust_id, calendar_dt,
            ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY calendar_dt)  row_num_transaction
FROM dailytransactions 
),
-- finding the consecutive days that the customer made purchases, assigning another row number but this time by customer id and by the diff between the unique row number and  the calendar date 
cons_days AS (
SELECT cust_id, ROW_NUMBER() OVER (PARTITION BY cust_id, TO_DATE(calendar_dt, 'mm/dd/yyyy') - row_num_transaction ORDER BY calendar_dt) consecutive_days
FROM unique_transaction
)
-- finding the max number of consecutive days a customer made purchases
SELECT cust_id, MAX(consecutive_days)  max_consecutive_days
FROM cons_days
GROUP BY cust_id;
-------------------------------------------------------------------------------------------------------------------------------------

QUERY 2:

-- calculating the average number of transactions it takes a customer to reach a spent threshold of 250 L.E
-- I assumed that a customer could reach the 250 L.E threshold only once and that the threshold could be reached 
-- either by a single transaction or multiple transactions
threshold of 250 L.E
WITH num_transactions AS (
  SELECT cust_id, MIN(TO_DATE(calendar_dt, 'mm/dd/yyyy')) AS first_date, COUNT(*) AS num_days
  FROM dailytransactions
  WHERE amt_LE >= 250
  GROUP BY cust_id
)
SELECT AVG(num_days) AS avg_days_to_reach_threshold
FROM num_transactions ;


