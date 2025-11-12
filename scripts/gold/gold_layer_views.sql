/*
===============================================================================
File Name   : gold_layer_views.sql
Database    : DataWarehouse
Schema      : gold
Author      : Hemanth Kumar Karthikeyan
Created On  : 12-Nov-2025
Version     : 1.0

Description :
    This script creates Gold layer views for the Data Warehouse. It includes:
    1) dim_customers: Dimension table for customers combining CRM and ERP info.
    2) dim_products: Dimension table for products with ERP category details.
    3) fact_sales: Fact table of sales linking products and customers.

Purpose:
    - Transform Silver-layer cleansed tables into analytical-ready Gold layer views.
    - Ensure key relationships (customer_key, product_key) for fact-dimension joins.
    - Filter and standardize data for reporting, analytics, and BI consumption.

Usage Notes:
    - These are views, not physical tables; they reflect the latest Silver layer data.
    - Recommended to run after Silver layer ETL is complete.
    - Only active products (prd_end_dt IS NULL) are included in dim_products.
===============================================================================
*/

-- ===========================================================================
-- View: dim_customers
-- ===========================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT		ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key, -- Surrogate key
			    ci.cst_id				      AS customer_id,
			    ci.cst_key				    AS customer_number,
			    ci.cst_firstname	    AS first_name,
			    ci.cst_lastname		    AS last_name,
			    cl.country				    AS country,
			    ci.cst_marital_status	AS marital_status,
			    CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr ELSE COALESCE(ca.gen,'N/A') END AS gender, -- CRM is the primary source for gender, Fallback to ERP if data is not present
			    ca.bdate				      AS birth_date,
			    ci.cst_create_date		AS create_date		
FROM		  [silver].[crm_cust_info] ci
LEFT JOIN	[silver].[erp_cust_az12] ca ON ca.cid = ci.cst_key
LEFT JOIN	[silver].[erp_loc_a101] cl ON cl.cid = ci.cst_key;

-- ===========================================================================
-- View: dim_products
-- ===========================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT      ROW_NUMBER() OVER (ORDER BY pn.[prd_start_dt], pn.[prd_key]) AS product_key, -- Surrogate key
            pn.[prd_id]         AS product_id,
            pn.[prd_key]        AS product_number,
            pn.[prd_nm]         AS product_name,
            pn.[prd_cat_id]     AS category_id,
            pc.[cat]            AS category,
            pc.[subcat]         AS subcategory,
            pc.[maintenance]    AS maintenance,
            pn.[prd_cost]       AS cost,
            pn.[prd_line]       AS product_line,
            pn.[prd_start_dt]   As start_date
FROM        [silver].[crm_prd_info] pn
LEFT JOIN   [silver].[erp_px_cat_g1v2] pc ON pn.prd_cat_id = pc.id
WHERE       pn.[prd_end_dt] IS NULL;                                 --- Fetching only currently active product data

-- ===========================================================================
-- View: fact_sales
-- ===========================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT      sl.[sls_ord_num]    AS order_number,
            pt.[product_key]    AS product_key,
            ct.[customer_key]   AS customer_key,
            sl.[sls_order_dt]   AS order_date,
            sl.[sls_ship_dt]    AS shipping_date,
            sl.[sls_due_dt]     AS due_date,
            sl.[sls_price]      AS price,
            sl.[sls_quantity]   AS quantity,
            sl.[sls_sales]      AS sales_amount            
FROM        [silver].[crm_sales_details] sl
LEFT JOIN   [gold].[dim_products] pt ON sl.sls_prd_key = pt.product_number
LEFT JOIN   [gold].[dim_customers] ct ON sl.sls_cust_id = ct.customer_id;
