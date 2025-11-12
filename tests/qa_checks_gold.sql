/*
===============================================================================
File Name   : qa_checks_gold.sql
Database    : DataWarehouse
Schema      : gold
Author      : Hemanth Kumar Karthikeyan
Created On  : 12-Nov-2025
Version     : 1.0

Description :
    This script performs quality checks on the Gold layer views to ensure data 
    consistency, uniqueness, and correct connectivity between fact and dimension tables.

Checks Included:
    1) Uniqueness of surrogate keys in dimension tables (dim_customers, dim_products)
    2) Referential integrity between fact_sales and dimension tables

Usage Notes:
    - Run this script after Gold layer views are created or refreshed.
    - Investigate any non-zero counts or NULL references returned by these checks.
===============================================================================
*/

-- ===========================================================================
-- Check 1: gold.dim_customers uniqueness
-- ===========================================================================
-- Purpose: Ensure customer_key is unique in the customer dimension
-- Expectation: No results (i.e., all customer_key values should be unique)

SELECT 		customer_key,
			    COUNT(*) AS duplicate_count   	-- Count of duplicates for each customer_key
FROM 		  gold.dim_customers
GROUP BY 	customer_key
HAVING 		COUNT(*) > 1;            		    -- Only return keys that appear more than once

-- ===========================================================================
-- Check 2: gold.dim_products uniqueness
-- ===========================================================================
-- Purpose: Ensure product_key is unique in the product dimension
-- Expectation: No results (i.e., all product_key values should be unique)

SELECT     	product_key,
			      COUNT(*) 	AS duplicate_count  -- Count of duplicates for each product_key
FROM 		    gold.dim_products
GROUP BY 	  product_key
HAVING 		  COUNT(*) > 1;            		  -- Only return keys that appear more than once

-- ===========================================================================
-- Check 3: gold.fact_sales referential integrity
-- ===========================================================================
-- Purpose: Ensure all fact_sales records reference valid customers and products
-- Expectation: No results (i.e., no fact row should have a missing dimension key)

SELECT 		  * 
FROM 		    gold.fact_sales f
LEFT JOIN 	gold.dim_customers c ON c.customer_key = f.customer_key   	-- Join to customer dimension
LEFT JOIN 	gold.dim_products p  ON p.product_key = f.product_key     	-- Join to product dimension
WHERE 		  p.product_key IS NULL                						            -- Product key missing in dimension
			      OR c.customer_key IS NULL;             						          -- Customer key missing in dimension
