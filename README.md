/*
=============================================================
SQL Advanced Data Analytics Project
=============================================================

Welcome to the **Advanced Data Analytics Project** repository.

🔹 This project focuses on performing **advanced data analytics using SQL** on a sales dataset.
🔹 The analysis includes **sales performance, cumulative analysis, performance analysis, part-to-whole analysis, and customer segmentation**.

🚀 **Technologies Used**: MySQL, SQL Queries, CTEs, Window Functions, Aggregate Functions, Joins.

=============================================================
🚨 WARNING:
    Running this script will **DROP the entire 'DataWarehouseAnalytics' database** if it exists.
    Ensure you have a **backup** before proceeding.
=============================================================
*/

-- =========================================================
-- 🏛️ Step 1: Create Database and Tables
-- =========================================================

DROP DATABASE IF EXISTS DataWarehouseAnalytics;
CREATE DATABASE DataWarehouseAnalytics;
USE DataWarehouseAnalytics;

-- 🎯 Create the Customers Dimension Table
CREATE TABLE dim_customers (
    customer_key INT PRIMARY KEY,
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

-- 🎯 Create the Products Dimension Table
CREATE TABLE dim_products (
    product_key INT PRIMARY KEY,
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

-- 🎯 Create the Fact Sales Table
CREATE TABLE fact_sales (
    order_number VARCHAR(50),
    product_key INT,
    customer_key INT,
    order_date DATE,
    shipping_date DATE,
    due_date DATE,
    sales_amount INT,
    quantity TINYINT,
    price INT,
    FOREIGN KEY (product_key) REFERENCES dim_products(product_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customers(customer_key)
);

-- ✅ Confirm that tables are created
SHOW TABLES;

-- =========================================================
-- 📊 Step 2: Sales Performance Analysis
-- =========================================================

-- 1️⃣ Total Sales by Date
SELECT order_date, SUM(sales_amount) AS total_sales
FROM fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_date
ORDER BY order_date;

-- 2️⃣ Total Sales by Year
SELECT 
    YEAR(order_date) AS order_year, 
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_year
ORDER BY order_year;

-- 3️⃣ Monthly Sales
SELECT 
    YEAR(order_date) AS order_year, 
    MONTH(order_date) AS order_month, 
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_year, order_month
ORDER BY order_year, order_month;

-- =========================================================
-- 📊 Step 3: Cumulative Sales Analysis
-- =========================================================

-- 🟢 Running Total of Sales Over Time
SELECT 
    DATE(order_date) AS order_date, 
    SUM(sales_amount) AS total_sales,
    SUM(SUM(sales_amount)) OVER (ORDER BY DATE(order_date)) AS cumulative_sales
FROM fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_date
ORDER BY order_date;

-- =========================================================
-- 📊 Step 4: Customer Segmentation Analysis
-- =========================================================

-- 🎯 Create Customer Segments Based on Spending
WITH customer_spending AS (
    SELECT 
        c.customer_key,
        SUM(f.sales_amount) AS total_spending,
        MIN(order_date) AS first_order,
        MAX(order_date) AS last_order,
        TIMESTAMPDIFF(MONTH, MIN(order_date), MAX(order_date)) AS life_span
    FROM fact_sales AS f
    LEFT JOIN dim_customers AS c ON f.customer_key = c.customer_key
    GROUP BY c.customer_key
)

SELECT 
    customer_key,
    total_spending,
    life_span,
    CASE 
        WHEN life_span >= 12 AND total_spending > 5000 THEN 'VIP'
        WHEN life_span >= 12 AND total_spending <= 5000 THEN 'Regular Customer'
        ELSE 'New'
    END AS customer_segment
FROM customer_spending
ORDER BY total_spending DESC;

-- =========================================================
-- 📊 Step 5: Customer Report - Creating a View
-- =========================================================

CREATE OR REPLACE VIEW customer_report AS
WITH base_query AS (
    SELECT  
        f.order_number,
        f.product_key,
        f.order_date,
        f.sales_amount,
        f.quantity,
        c.customer_key,
        c.customer_number,
        TIMESTAMPDIFF(YEAR, c.birthdate, CURDATE()) AS age,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name
    FROM fact_sales AS f  
    LEFT JOIN dim_customers AS c ON c.customer_key = f.customer_key
),

customer_aggregation AS (
    SELECT  
        customer_key,
        customer_number,
        customer_name,
        age,
        COUNT(DISTINCT order_number) AS total_orders,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        COUNT(DISTINCT product_key) AS total_products,
        MAX(order_date) AS last_order,
        TIMESTAMPDIFF(MONTH, MIN(order_date), MAX(order_date)) AS life_span,
        SUM(sales_amount) AS total_spending
    FROM base_query
    GROUP BY  
        customer_key,
        customer_number,
        customer_name,
        age
)

SELECT  
    customer_key,
    customer_number,
    customer_name,
    age,
    CASE 
        WHEN age < 20 THEN 'Under 20'
        WHEN age BETWEEN 20 AND 29 THEN '20 - 29'
        WHEN age BETWEEN 30 AND 39 THEN '30 - 39'
        WHEN age BETWEEN 40 AND 49 THEN '40 - 49'
        ELSE '50 and Above'
    END AS age_group,
    CASE 
        WHEN life_span >= 12 AND total_spending > 5000 THEN 'VIP'
        WHEN life_span >= 12 AND total_spending <= 5000 THEN 'Regular Customer'
        ELSE 'New'
    END AS customer_segment,
    last_order,
    TIMESTAMPDIFF(MONTH, last_order, CURDATE()) AS recency,
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    life_span,
    CASE 
        WHEN total_orders = 0 THEN 0
        ELSE ROUND(total_sales / total_orders,0)
    END AS Average_value,
    CASE 
        WHEN life_span = 0 THEN total_sales
        ELSE ROUND(total_sales / life_span,0)
    END AS avg_monthly_spend

FROM customer_aggregation;

-- ✅ To view the final report
SELECT * FROM customer_report;

