# SQL-Advance-Data-Analytics-Project

=============================================================

Welcome to the ** Advance Data Analytical Project ** repository
/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- Drop and recreate the 'DataWarehouseAnalytics' database
DROP DATABASE IF EXISTS DataWarehouseAnalytics;
CREATE DATABASE DataWarehouseAnalytics;
USE DataWarehouseAnalytics;

-- Create Tables (No separate Schema concept in MySQL)
CREATE TABLE dim_customers (
    customer_key INT,
    customer_id INT,
    customer_number VARCHAR(50),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    country VARCHAR(50),
    marital_status VARCHAR(50),
    gender VARCHAR(50),
    birthdate DATE,
    create_date DATE
);

CREATE TABLE dim_products (
    product_key INT,
    product_id INT,
    product_number VARCHAR(50),
    product_name VARCHAR(50),
    category_id VARCHAR(50),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    maintenance VARCHAR(50),
    cost INT,
    product_line VARCHAR(50),
    start_date DATE
);

CREATE TABLE fact_sales (
    order_number VARCHAR(50),
    product_key INT,
    customer_key INT,
    order_date DATE,
    shipping_date DATE,
    due_date DATE,
    sales_amount INT,
    quantity TINYINT,
    price INT
);

-- Truncate tables before loading data
-- TRUNCATE TABLE dim_customers;
-- TRUNCATE TABLE dim_products;
-- TRUNCATE TABLE fact_sales;

USE DataWarehouseAnalytics;


show tables;


SELECT COUNT(*) FROM dim_products;
SELECT COUNT(*) FROM fact_sales;
SELECT COUNT(*) FROM dim_customers;

-- 1.  Analyze sales performance over time

select * from fact_sales;

-- Total sales by date

select order_date, SUM(sales_amount) as total_sales
from fact_sales
where order_date is not null
group by order_date
order by order_date;

-- Total sales by year

select year(order_date) as order_year, SUM(sales_amount) as total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
sum(quantity) as total_quantity
from fact_sales
where order_date is not null
group by order_year
order by order_year;

-- Total sales by month

select month(order_date) as order_month, SUM(sales_amount) as total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
sum(quantity) as total_quantity
from fact_sales
where order_date is not null
group by order_month
order by order_month;

-- Combined year and month 

select year(order_date) as order_year, month(order_date) as order_month, SUM(sales_amount) as total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
sum(quantity) as total_quantity
from fact_sales
where order_date is not null
group by order_year, order_month
order by order_year, order_month;

-- full year sales

SELECT DATE(order_date) AS full_date,  
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM fact_sales
WHERE order_date IS NOT NULL
GROUP BY full_date
ORDER BY full_date;

-- 2. CUMULATIVE ANALYSIS

-- calculate total sales of each month

select DATE(order_date) as order_date, sum(sales_amount) as total_sales
from fact_sales
where order_date is not null
group by order_date
order by order_date;

-- Running total of sales over time 

SELECT 
    DATE(order_date) AS order_date, 
    SUM(sales_amount) AS total_sales,
    -- avg(price) as avg_price,
    SUM(SUM(sales_amount)) OVER (ORDER BY DATE(order_date)) AS cumulative_sales,
    avg(avg(price)) OVER (ORDER BY DATE(order_date)) AS cumulative_average_price
FROM fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_date
ORDER BY order_date;

-- yearly cumulative 

SELECT 
    YEAR(order_date) AS order_year,
    DATE(order_date) AS full_date,
    SUM(sales_amount) AS total_sales,
    SUM(SUM(sales_amount)) OVER (PARTITION BY YEAR(order_date) ORDER BY order_date) AS cumulative_sales
FROM fact_sales
WHERE order_date IS NOT NULL
GROUP BY full_date, order_year
ORDER BY order_year, full_date;


-- 3. PERFORMANCE ANALYSIS

-- Analyze the yearly performance of products by comparing thier sales 
-- to both the average sales performance of the product and the previous year sales

-- OUTPUT : I Took the Attributes what i need and join them  and create new attributes for the differences between current sales and average sales
-- 			and differences between curreent year sales and previour year sales


-- By using joins I extract the tables what i need for the Analysis

select year(f.order_date) as order_year, 
p.product_name, 
sum(f.sales_amount) as current_sales
from DataWarehouseAnalytics.fact_sales as f 
left join DataWarehouseAnalytics.dim_products as p
on f.product_key = p.product_key
where order_date is not null
group by order_year, product_name
order by order_date;

-- By using CTE i call this in short form for further analysis 

with yearly_product_sales as (
select year(f.order_date) as order_year, 
p.product_name, 
sum(f.sales_amount) as current_sales
from DataWarehouseAnalytics.fact_sales as f 
left join DataWarehouseAnalytics.dim_products as p
on f.product_key = p.product_key
where order_date is not null
group by order_year, product_name
order by order_date)

-- Here i am creating new Attribute for current sales and average sales 

select order_year, 
product_name, 
current_sales,
cast(avg(current_sales) over (partition by product_name)as signed) as avg_sales,
current_sales - cast(avg(current_sales) over (partition by product_name)as signed) as diff_avg,

-- I used Case functioin for the easy finding of difference between current sales and average sales

case 
when current_sales - cast(avg(current_sales) over (partition by product_name)as signed) > 0 then 'Above Avg'
when current_sales - cast(avg(current_sales) over (partition by product_name)as signed) < 0 then 'Below Avg'
else 'Avg'
end as Agv_change,

-- I used Lag Function to chcek the difference between curreent year sales and previour year sales

lag(current_sales) over (partition by product_name order by order_year) as previous_year_sales,
current_sales - lag(current_sales) over (partition by product_name order by order_year) as diff_py,

-- Case function to clear sight for the difference 

case
when current_sales - lag(current_sales) over (partition by product_name order by order_year) > 0 then 'Increase'
when current_sales - lag(current_sales) over (partition by product_name order by order_year) < 0 then 'Decrease'
else 'NO'
end as Agv_py
 from yearly_product_sales
 order by product_name, order_year;

-- 4. Part-to- Whole Analysis

-- which category contribute most overall sales 

select category, 
sum(sales_amount) as total_sales
from DataWarehouseAnalytics.fact_sales as f 
left join DataWarehouseAnalytics.dim_products as p
on f.product_key = p.product_key
group by category;

-- will use CTE for the further Analysis

with category_sales as (
select category, 
sum(sales_amount) as total_sales
from DataWarehouseAnalytics.fact_sales as f 
left join DataWarehouseAnalytics.dim_products as p
on f.product_key = p.product_key
group by category)

-- I find the percentage of total sales  by each. category
select 
category,
total_sales,
sum(total_sales) over() as overall_sales,
concat(round((total_sales / sum(total_sales) over()) * 100, 2), '%') as percentage_of_total_sales
from category_sales
order by total_sales desc;

-- 5 Data Segmentation

-- 1.segment products into cost ranges and 
-- 2.count how many products fall into each segment

-- 1.segment products into cost ranges

with product_segment as (
select product_key,
product_name, 
cost,
case 
	when cost < 100 then 'Below 100'
	when cost between 100 and 500 then '100-500'
	when cost between 500 and 1000 then '500-1000'
	else 'above 1000'
end as cost_range
from dim_products)

-- 2.count how many products fall into each segment

select 
cost_range,
count(product_key) as total_products
from product_segment
group by cost_range
order by total_products desc;

-- Another Task (Group customers into three segments by their spending habits)

select 
c.customer_key,
f.sales_amount,
f.order_date
from DataWarehouseAnalytics.fact_sales as f
left join DataWarehouseAnalytics.dim_customers as c 
on f.customer_key = c.customer_key;

-- we have to seperate the customers according to life span 

with customer_spending as (
select 
c.customer_key,
sum(f.sales_amount) as total_spending,
min(order_date) as first_order,
max(order_date) as last_order,

-- for the lifespan of the customer we use TIMESTAMPDIFF to know the difference

TIMESTAMPDIFF (month, min(order_date),max(order_date)) as life_span
from DataWarehouseAnalytics.fact_sales as f
left join DataWarehouseAnalytics.dim_customers as c 
on f.customer_key = c.customer_key
group by c.customer_key
),

-- NOTE - Below i did TWO Methods using another cte and sub query to get the customer_segment either one can use by comment out the other one

-- method 1 = Find the customers_segment by using another cte 
final_result as 
(select 
customer_key,
total_spending,
life_span,
case 
	when life_span >= 12 and total_spending > 5000 then 'vip_'
    when life_span >= 12 and total_spending <= 5000 then 'Regular_customers'
    else 'New'
end as customer_segment
from customer_spending)


select 
customer_segment,
count(customer_key) as total_customers
from final_result
group by customer_segment 
order by total_customers desc;

-- method 2 = Finding custromer_segment by using sub query

select 
customer_segment,
count(customer_key) as total_customers
from (
select 
customer_key,
case 
	when life_span >= 12 and total_spending > 5000 then 'vip_'
    when life_span >= 12 and total_spending <= 5000 then 'Regular_customers'
    else 'New'
end as customer_segment
from customer_spending) t
group by customer_segment 
order by total_customers desc;


-- Build customer report

-- Retrieve columns what i need 

create view customer_report as 

with base_query as (
select 
f.order_number,
f.product_key,
order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
TIMESTAMPDIFF(YEAR, c.birthdate, CURDATE()) AS age,
concat(c.first_name, ' ', c.last_name) as customer_name
from DataWarehouseAnalytics.fact_sales as f 
left join DataWarehouseAnalytics.dim_customers as c
on c.customer_key = f.customer_key
),

-- Necessary Aggregations 

customer_aggregation as (
select 
customer_key,
customer_number,
customer_name,
age,
count(distinct order_number) as total_orders,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity,
count(distinct product_key) as total_products,
max(order_date) as last_order,
TIMESTAMPDIFF(MONTH, MIN(order_date), MAX(order_date)) AS life_span,
 SUM(sales_amount) AS total_spending
from base_query
group by 
customer_key,
customer_number,
customer_name,
age
)

-- final result 

select
customer_key,
customer_number,
customer_name,
age,
case 
	when age < 20 then 'under 20'
    when age between 20 and 29 then '20 -29'
    when age between 30 and 39 then '30 -39'
    when age between 40 and 49 then '40 -49'
    else '50  and Above'
end as age_group,

case 
	when life_span >= 12 and total_spending > 5000 then 'vip_'
    when life_span >= 12 and total_spending <= 5000 then 'Regular_customers'
    else 'New'
end as customer_segment,
last_order,
TIMESTAMPDIFF(MONTH, last_order, CURDATE()) as recency,
total_orders,
total_sales,
total_quantity,
total_products,
life_span,

-- Calculate average order value
case 
	when total_orders = 0 then 0
    else round(total_sales / total_orders,0)
end as Average_value,

-- calculate the average monthly spend 
case 
	when life_span = 0 then total_sales
    else round (total_sales / life_span,0)
end as avg_monthly_spend

from customer_aggregation;


-- FINAL REPORT

select *
from customer_report;


## License
This project is licensed under the [MIT License] (License). you are free to use, modify and share this project with proper attribution


























