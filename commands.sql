-- How many stores does the business have and in which countries?

SELECT COUNT(DISTINCT store_code) AS num_stores, country_code
FROM dim_store_details
GROUP BY country_code;

-- Which locations have the most stores?

SELECT locality, COUNT(store_code) AS num_stores
FROM dim_store_details
GROUP BY locality
ORDER BY num_stores DESC

-- Which months on average produce the highest cost of sales?

SELECT 
  dim_date_times.month, 
  SUM(orders_table.product_quantity * dim_products.product_price) AS avg_cost_of_sales
FROM 
  orders_table 
  JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
  JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY 
  dim_date_times.month
ORDER BY 
  avg_cost_of_sales DESC;

-- How many sales are coming from online?

SELECT
  dim_store_details.store_type,
  SUM(orders_table.product_quantity) AS total_products_sold,
  COUNT(DISTINCT orders_table.date_uuid) AS total_sales
FROM
  orders_table
  JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY
  dim_store_details.store_type;

-- What precentage of sales come through each type of store?

SELECT
    s.store_type,
    SUM(p.product_price * o.product_quantity) AS total_sales,
    SUM(p.product_price * o.product_quantity) / sum(sum(p.product_price * o.product_quantity)) OVER() * 100 AS percentage_sales
FROM 
    orders_table o
    JOIN dim_store_details s ON o.store_code = s.store_code
    JOIN dim_products p ON o.product_code = p.product_code
GROUP BY 
    s.store_type
ORDER BY 
    total_sales DESC;

-- Which month in each year produced the highest cost of sales?

SELECT dim_date_times.year, dim_date_times.month, round(SUM(orders_table.product_quantity * dim_products.product_price)) AS total_sales
FROM orders_table
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY dim_date_times.month, dim_date_times.year
ORDER BY total_sales DESC;

-- What is our staff headcount?

SELECT country_code, SUM(staff_numbers) AS total_staff_numbers
FROM dim_store_details
GROUP BY country_code;

-- Which German store type is selling the most?

SELECT dim_store_details.store_type, SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales
FROM orders_table
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN dim_products ON orders_table.product_code = dim_products.product_code
WHERE dim_store_details.country_code = 'DE'
GROUP BY dim_store_details.store_type
ORDER BY total_sales Asc

-- How quickly is the company making sales?

SELECT year, AVG(diff) as actual_time_taken
FROM (
	SELECT year, LEAD(my_timestamp) OVER (ORDER BY year) - my_timestamp AS diff
	FROM (
		SELECT year, to_timestamp(year || '-' || month || '-' || day || ' ' || timestamp, 'YYYY-MM-DD HH24:MI:SS') AT TIME ZONE 'UTC' AS my_timestamp
		FROM dim_date_times
		order by my_timestamp
	) subquery
) subquery
GROUP BY year
ORDER BY actual_time_taken DESC;