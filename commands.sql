

-- Altering data types of table 'orders_table'

SELECT MAX(LENGTH(CAST(card_number as TEXT))) FROM orders_table = max_command_card_number
SELECT MAX(LENGTH(store_code)) FROM orders_table = max_command_store_code
SELECT MAX(LENGTH(product_code)) FROM orders_table = max_command_product_code
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN card_number TYPE VARCHAR({max_command_card_number}),
ALTER COLUMN store_code TYPE VARCHAR({max_command_store_code}),
ALTER COLUMN product_code TYPE VARCHAR({max_command_product_code})

-- Altering data types of table 'dim_users'

SELECT MAX(LENGTH(country_code)) FROM dim_users = max_command_country_code
ALTER TABLE dim_users 
ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN date_of_birth TYPE DATE,
ALTER COLUMN country_code TYPE VARCHAR({max_command_country_code}),
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN join_date TYPE DATE

-- Altering data types of table 'dim_store_details'

SELECT MAX(LENGTH(country_code)) FROM dim_store_details = max_command_country_code2
SELECT MAX(LENGTH(store_code)) FROM dim_store_details = max_command_store_code2
ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT,
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN store_code TYPE VARCHAR({max_command_store_code2}),
ALTER COLUMN staff_numbers TYPE SMALLINT,
ALTER COLUMN opening_date TYPE DATE,
ALTER COLUMN store_type TYPE VARCHAR(255), DROP NOT NULL,
ALTER COLUMN latitude TYPE FLOAT,
ALTER COLUMN country_code TYPE VARCHAR({max_command_country_code2}),
ALTER COLUMN continent TYPE VARCHAR(255)


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