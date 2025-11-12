/*
===============================================================================
File Name   : qa_checks_silver.sql
Database    : DataWarehouse
Schema      : silver
Author      : Hemanth Kumar Karthikeyan
Created On  : 11-Nov-2025
Version     : 1.0

Description :
    This script performs data quality checks on the Silver-layer tables. The checks
    validate common data quality dimensions such as uniqueness, nulls, format
    consistency, range checks, and logical consistency across related fields.

Execution / Usage Notes :
    - Run this script AFTER the Silver layer load process completes.
    - Expected behavior: each check should return zero rows (no issues). Any row
      returned indicates a violation that requires investigation.
    - For production, consider wrapping these checks into a stored procedure that
      writes results to a QA/audit table and raises alerts when thresholds are exceeded.

Output / Remediation:
    - Each check contains a short remediation suggestion (in comments) to guide fixes.
    - Common remediation actions: fix source transforms in Bronze → re-run Silver transform,
      update parsing logic, or mark/flag bad records for manual review.

===============================================================================
*/

-- ===========================================================================
-- CHECKS: silver.crm_cust_info
-- ===========================================================================

-- 1) Check for NULLs or duplicate primary keys (cst_id).
--    Purpose: ensure the customer identifier is unique and not null.
--    Expectation: No rows returned.
--    Remediation: Identify duplicates, inspect source (bronze) for multiple records per customer; 
--				   decide deduplication rule (latest, best-quality, etc.).

SELECT      [cst_id],
            COUNT(*) 
FROM        [silver].[crm_cust_info]
GROUP BY    [cst_id]
HAVING      COUNT(*) > 1 
            OR [cst_id] IS NULL;

-- 2) Check for unwanted leading/trailing spaces in keys (cst_key).
--    Purpose: detect whitespace issues that will break joins.
--    Expectation: No rows returned.
--    Remediation: Trim in transform; consider creating normalized key column for joins.

SELECT  [cst_key]
FROM    [silver].[crm_cust_info]
WHERE   [cst_key] != TRIM([cst_key]);

-- 3) Check standardization of marital status values.
--    Purpose: verify only expected values are present (e.g., 'Single','Married','N/A').
--    Expectation: Only expected values appear.
--    Remediation: Map unexpected values in transform or add mapping table and reprocess.

SELECT  DISTINCT [cst_marital_status] 
FROM    [silver].[crm_cust_info];

-- ====================================================================
-- CHECKS: silver.crm_prd_info
-- ====================================================================

-- 4) Check for NULLs or duplicates in product id (prd_id).
--    Purpose: ensure stable primary key for products.
--    Expectation: No rows returned.
--    Remediation: Investigate Bronze feed for missing IDs; generate surrogate IDs if necessary.

SELECT      [prd_id],
            COUNT(*) 
FROM        [silver].[crm_prd_info]
GROUP BY    [prd_id]
HAVING      COUNT(*) > 1 
            OR [prd_id] IS NULL;

-- 5) Check for unwanted spaces in product name (prd_nm).
--    Purpose: ensure names are trimmed for consistent reporting and joins.
--    Expectation: No rows returned.
--    Remediation: Trim in transform; consider normalizing casing if needed.

SELECT  [prd_nm]
FROM    [silver].[crm_prd_info]
WHERE   [prd_nm] != TRIM([prd_nm]);

-- 6) Check for NULLs or negative values in product cost (prd_cost).
--    Purpose: ensure cost values are present and non-negative.
--    Expectation: No rows returned.
--    Remediation: If cost is missing, decide default or flag product for review. If negative, correct sign.

SELECT  [prd_cost]
FROM    [silver].[crm_prd_info]
WHERE   [prd_cost] < 0 
        OR [prd_cost] IS NULL;

-- 7) Check distinct prd_line values for standardization.
--    Purpose: identify unexpected product-line values (should be mapped to known set).
--    Expectation: Only allowed values (e.g., 'Mountain','Road','Touring','Other Sales','N/A').
--    Remediation: Add mapping rule for unexpected values in transform.

SELECT  DISTINCT [prd_line] 
FROM    [silver].[crm_prd_info];

-- 8) Check for invalid date order: prd_end_dt earlier than prd_start_dt.
--    Purpose: detect incorrect product lifecycle dates.
--    Expectation: No rows returned.
--    Remediation: Recompute end_date logic in transform (LEAD logic) or inspect source.

SELECT  * 
FROM    silver.crm_prd_info
WHERE   [prd_end_dt] < [prd_start_dt];

-- ====================================================================
-- CHECKS: silver.crm_sales_details
-- ====================================================================

-- 9) Check for invalid date values in bronze (source) - exploratory check.
--    Purpose: find obviously invalid integer-date formats in Bronze before they are converted.
--    Expectation: Ideally none; if found, handle in transform.
--    Remediation: Fix parsing logic; reject or flag invalid date rows.

SELECT  NULLIF([sls_due_dt], 0) AS [sls_due_dt] 
FROM    [bronze].[crm_sales_details]
WHERE   [sls_due_dt] <= 0 
        OR LEN([sls_due_dt]) != 8 
        OR [sls_due_dt] > 20500101 
        OR [sls_due_dt] < 19000101;

-- 10) Check for invalid date orders in sales: order date after ship/due date.
--     Purpose: ensure chronological integrity (order <= ship <= due).
--     Expectation: No rows returned.
--     Remediation: Inspect records and transform which produced dates; correct source if necessary.

SELECT  * 
FROM    [silver].[crm_sales_details]
WHERE   [sls_order_dt] > [sls_ship_dt] 
        OR [sls_order_dt] > [sls_due_dt];

-- 11) Check for sales quantity/price/sales consistency: sales = quantity * price
--     Purpose: ensure derived totals align with quantity and unit price.
--     Expectation: No rows returned.
--     Remediation: Recompute sales in transform where inconsistent; set audit flag for manual review.

SELECT      DISTINCT [sls_sales],
            [sls_quantity],
            [sls_price] 
FROM        [silver].[crm_sales_details]
WHERE       [sls_sales] != [sls_quantity] * [sls_price]
            OR [sls_sales] IS NULL 
            OR [sls_quantity] IS NULL 
            OR [sls_price] IS NULL
            OR [sls_sales] <= 0 
            OR [sls_quantity] <= 0 
            OR [sls_price] <= 0
ORDER BY    [sls_sales], 
            [sls_quantity], 
            [sls_price];

-- ====================================================================
-- CHECKS: silver.erp_cust_az12
-- ====================================================================

-- 12) Identify out-of-range birthdates.
--     Purpose: ensure birthdates are plausible (e.g., between 1924-01-01 and today).
--     Expectation: No rows returned.
--     Remediation: Set invalid bdate to NULL or verify source feed and correct.

SELECT  DISTINCT [bdate]
FROM    [silver].[erp_cust_az12]
WHERE   [bdate] < '1924-01-01' 
        OR [bdate] > GETDATE();

-- 13) Check gender (gen) standardization in ERP customers.
--     Purpose: ensure gender values are standardized (e.g., 'Male','Female','N/A').
--     Expectation: Only expected values appear.
--     Remediation: Map unexpected values during transform; add mapping reference table if needed.

SELECT  DISTINCT [gen] 
FROM    [silver].[erp_cust_az12];

-- ====================================================================
-- CHECKS: silver.erp_loc_a101
-- ====================================================================

-- 14) Check country standardization values.
--     Purpose: ensure country labels are normalized for downstream reporting.
--     Expectation: Only allowed normalized country values (e.g., 'USA','Germany','France','Canada','UK','Australia','N/A').
--     Remediation: Add mapping or dictionary table if more countries need consistent names.

SELECT      DISTINCT [country] 
FROM        [silver].[erp_loc_a101]
ORDER BY    [country];

-- ====================================================================
-- CHECKS: silver.erp_px_cat_g1v2
-- ====================================================================

-- 15) Check for unwanted spaces in text columns (cat, subcat, maintenance).
--     Purpose: ensure no leading/trailing whitespace that breaks categorization or grouping.
--     Expectation: No rows returned.
--     Remediation: Trim in transform; consider storing normalized columns (lower/upper) for consistency.

SELECT  * 
FROM    [silver].[erp_px_cat_g1v2]
WHERE   [cat] != TRIM([cat]) 
        OR [subcat] != TRIM([subcat]) 
        OR [maintenance] != TRIM([maintenance]);

-- 16) Check distinct maintenance values to validate domain.
--     Purpose: identify unexpected maintenance flags that may require mapping.
--     Expectation: Only known values appear.
--     Remediation: Standardize values in transform or create a maintenance reference table.

SELECT  DISTINCT [maintenance] 
FROM    [silver].[erp_px_cat_g1v2];
