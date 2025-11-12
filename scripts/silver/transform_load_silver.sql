/***************************************************************************************************
File Name   : transform_load_silver.sql
Database    : DataWarehouse
Schema      : silver
Author      : Hemanth Kumar Karthikeyan
Created On  : 11-Nov-2025
Version     : 1.0

Description :
This script performs the data transformation from the Bronze Layer (raw data) to the Silver Layer 
(cleaned and standardized data) within the Data Warehouse.

The Silver Layer is responsible for:
- Cleaning and standardizing data formats.
- Removing duplicates and invalid records.
- Applying business rules and transformations.
- Ensuring referential and structural consistency for downstream layers.

The process involves:
1. Truncating the Silver Layer tables to prepare for fresh load.
2. Transforming and inserting data from corresponding Bronze tables.
3. Applying cleaning logic such as trimming text, normalizing codes, handling nulls, 
   correcting invalid dates, and ensuring consistent categorical values.

Transformations Applied :
--------------------------
CRM Tables:
------------
1. silver.crm_cust_info
   - Removes duplicates using ROW_NUMBER().
   - Expands marital status codes (‘S’, ‘M’) into readable values.
   - Normalizes gender codes (‘M’, ‘F’) to ‘Male’, ‘Female’.
   - Trims whitespace in names.
   - Keeps the latest record per customer.

2. silver.crm_prd_info
   - Derives product category ID from product key.
   - Normalizes product line abbreviations to descriptive names.
   - Handles null/zero product cost values.
   - Derives product end date using LEAD() logic.

3. silver.crm_sales_details
   - Validates and converts date fields (order, ship, due dates).
   - Recomputes sales if inconsistent with quantity × price.
   - Corrects invalid or null prices.
   - Orders by quantity for audit/debugging.

ERP Tables:
------------
4. silver.erp_cust_az12
   - Cleans customer IDs (removes 'NAS' prefix).
   - Validates birthdates (removes future dates).
   - Standardizes gender values.

5. silver.erp_loc_a101
   - Cleans customer IDs (removes hyphens).
   - Standardizes country names into unified categories.

6. silver.erp_px_cat_g1v2
   - Direct copy as reference data, no transformation needed.

Usage :
--------
Run this script after the Bronze Layer data load (stored procedure: [bronze].[load_data_bronze]) 
to populate the Silver Layer with cleaned and standardized datasets.

Notes :
--------
- Ensure both [bronze] and [silver] schemas exist and all source Bronze tables are populated.
- Running this script will truncate existing Silver data before each insert.
- This transformation layer acts as input for the Gold Layer (business-ready data).
***************************************************************************************************/
USE [DataWarehouse]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER PROCEDURE [silver].[transform_load_silver]
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_date DATETIME, @batch_end_date DATETIME
    BEGIN TRY
        SET NOCOUNT ON;

        SET @batch_start_date = GETDATE();

		PRINT '=================================================================';
		PRINT 'Starting Silver Layer Data Transfomation and Load Process...';
		PRINT '=================================================================';
		PRINT '';

		----------------------------------------
		-- Transform and Load CRM Customer Info
		----------------------------------------

		PRINT '--------------------------------------'
		PRINT '--- Transform and Load CRM Tables ---'
		PRINT '--------------------------------------'
		PRINT '';
		
		SET @start_time = GETDATE()
		PRINT 'Step 1: Loading CRM Customer Info data...';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Table silver.crm_cust_info truncated successfully.';
		
		PRINT '>> Inserting Data into silver.crm_cust_info Table...';
		INSERT INTO silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
		SELECT cst_id, 
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN cst_marital_status = 'S' THEN 'Single'
			 WHEN cst_marital_status = 'M' THEN 'Married'
			 ELSE 'N/A' END AS cst_marital_status,
		CASE WHEN cst_gndr = 'M' THEN 'Male'
			 WHEN cst_gndr = 'F' THEN 'Female'
			 ELSE 'N/A' END AS cst_gndr,
		cst_create_date
		FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS latest FROM [bronze].[crm_cust_info] WHERE cst_id IS NOT NULL)t
		WHERE latest = 1
		PRINT '>> Total Records Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' records.';
		SET @end_time = GETDATE()
        PRINT '>> CRM Customer Info data is transformed and loaded successfully into silver.crm_cust_info Table.';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
        PRINT '';
		
		---------------------------------------
        -- Transform and Load CRM Product Info
        ---------------------------------------

        SET @start_time = GETDATE()
        PRINT 'Step 2: Loading CRM Product Info data...';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Table silver.crm_prd_info truncated successfully.';

        PRINT '>> Inserting Data into silver.crm_prd_info Table...';
		INSERT INTO silver.crm_prd_info ([prd_id],[prd_cat_id],[prd_key],[prd_nm],[prd_cost],[prd_line],[prd_start_dt],[prd_end_dt])
		SELECT [prd_id]
			  ,REPLACE(SUBSTRING([prd_key],1,5),'-','_') AS [prd_cat_id]
			  ,SUBSTRING([prd_key], 7,LEN([prd_key])) AS [prd_key]
			  ,[prd_nm]
			  ,ISNULL([prd_cost],0) AS [prd_cost]
			  ,CASE UPPER(TRIM([prd_line]))   
					WHEN 'M' THEN 'Mountain'
					WHEN 'R' THEN 'Road'
					WHEN 'S' THEN 'Other Sales'
					WHEN 'T' THEN 'Touring'
					ELSE 'N/A' END AS [prd_line]
			  ,CAST([prd_start_dt] AS DATE) AS [prd_start_dt]
			  ,CAST(LEAD([prd_start_dt]) OVER (PARTITION BY [prd_key] ORDER BY [prd_start_dt])-1 AS DATE) AS [prd_end_dt]
		FROM [DataWarehouse].[bronze].[crm_prd_info]
		PRINT '>> Total Records Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' records.';
        SET @end_time = GETDATE()
        PRINT '>> CRM Product Info data is transformed and loaded successfully into silver.crm_prd_info Table.';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
        PRINT '';

        ------------------------------------------
        --  Transform and Load CRM Sales Details
        ------------------------------------------

        SET @start_time = GETDATE()
        PRINT 'Step 3: Loading CRM Sales Details data...';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Table silver.crm_sales_details truncated successfully.';

        PRINT '>> Inserting Data into silver.crm_sales_details Table...';
		INSERT INTO silver.crm_sales_details([sls_ord_num],[sls_prd_key],[sls_cust_id],[sls_order_dt],[sls_ship_dt],[sls_due_dt],[sls_sales],[sls_quantity],[sls_price])
		SELECT [sls_ord_num]
			  ,[sls_prd_key]
			  ,[sls_cust_id]
			  ,CASE WHEN [sls_order_dt] = 0 OR LEN([sls_order_dt]) != 8 THEN NULL
					ELSE CAST(CAST([sls_order_dt] AS VARCHAR) AS DATE) END AS [sls_order_dt]
			  ,CASE WHEN [sls_ship_dt] = 0 OR LEN([sls_ship_dt]) != 8 THEN NULL
					ELSE CAST(CAST([sls_ship_dt] AS VARCHAR) AS DATE) END AS [sls_ship_dt]
			  ,CASE WHEN [sls_due_dt] = 0 OR LEN([sls_due_dt]) != 8 THEN NULL
					ELSE CAST(CAST([sls_due_dt] AS VARCHAR) AS DATE) END AS [sls_due_dt]
			  ,CASE WHEN [sls_sales] IS NULL OR [sls_sales] <= 0 OR [sls_sales] != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
					ELSE [sls_sales] END AS [sls_sales]
			  ,[sls_quantity]
			  ,CASE WHEN [sls_price] IS NULL THEN [sls_sales]/NULLIF(sls_quantity, 0)
					WHEN [sls_price] <= 0 THEN ABS([sls_price])
					ELSE [sls_price] END AS [sls_price]
		FROM [DataWarehouse].[bronze].[crm_sales_details]
		ORDER BY sls_quantity DESC
		PRINT '>> Total Records Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' records.';
        SET @end_time = GETDATE()
        PRINT '>> CRM Sales Details data is transformed and loaded successfully into silver.crm_sales_details Table.';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
        PRINT '';

        -------------------------------------------
        -- Transform and Load ERP Customer Data
        -------------------------------------------

        PRINT '---------------------------------------'
        PRINT '--- Transform and Load ERP Tables ---'
        PRINT '---------------------------------------'
        PRINT '';

        SET @start_time = GETDATE()
        PRINT 'Step 4: Loading ERP Customer (CUST_AZ12) data...';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Table silver.erp_cust_az12 truncated successfully.';

        PRINT '>> Inserting Data into silver.erp_cust_az12 Table...';
		INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
		SELECT CASE WHEN SUBSTRING([cid],1,3) LIKE 'NAS%' THEN SUBSTRING([cid],4,LEN(cid))
					ELSE cid END AS cid
			  ,CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END AS bdate
			  ,CASE WHEN UPPER(TRIM([gen])) IN ('M', 'Male') THEN 'Male'
					WHEN UPPER(TRIM([gen])) IN ('F', 'Female') THEN 'Female'
					ELSE 'N/A' END AS [gen]
		FROM [DataWarehouse].[bronze].[erp_cust_az12]
		PRINT '>> Total Records Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' records.';
        SET @end_time = GETDATE()
        PRINT '>> ERP Customer data is transformed and loaded successfully into silver.erp_cust_az12 Table.';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
        PRINT '';

        ----------------------------------------
        -- Transform and Load ERP Location Data
        ----------------------------------------

        SET @start_time = GETDATE()
        PRINT 'Step 5: Loading ERP Location (LOC_A101) data...';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Table silver.erp_loc_a101 table successfully.';

        PRINT '>> Inserting Data into silver.erp_loc_a101 Table...';
		INSERT INTO silver.erp_loc_a101 (cid, country)
		SELECT REPLACE([cid],'-','') as cid
			  ,CASE WHEN UPPER(TRIM(country)) IN ('USA', 'UNITED STATES', 'US') THEN 'USA'
					WHEN UPPER(TRIM(country)) IN ('DE', 'GERMANY') THEN 'Germany'
					WHEN UPPER(TRIM(country)) = 'FRANCE' THEN 'France'
					WHEN UPPER(TRIM(country)) = 'CANADA' THEN 'Canada'
					WHEN UPPER(TRIM(country)) = 'UNITED KINGDOM' THEN 'UK'
					WHEN UPPER(TRIM(country)) = 'AUSTRALIA' THEN 'Australia'
					ELSE 'N/A' END AS country
		FROM [DataWarehouse].[bronze].[erp_loc_a101]
		PRINT '>> Total Records Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' records.';
        SET @end_time = GETDATE()
        PRINT '>> ERP Location data is transformed and loaded successfully into silver.erp_loc_a101 Table.';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
        PRINT '';

        -------------------------------------------------
        -- Transform and Load ERP Product Category Data
        -------------------------------------------------

        SET @start_time = GETDATE()
        PRINT 'Step 6: Loading ERP Product Category (PX_CAT_G1V2) data...';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Table silver.erp_px_cat_g1v2 truncated successfully.';

        PRINT '>> Inserting Data into silver.erp_px_cat_g1v2 Table...';
		INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		SELECT [id]
			  ,[cat]
			  ,[subcat]
			  ,[maintenance]
		FROM [DataWarehouse].[bronze].[erp_px_cat_g1v2]
		PRINT '>> Total Records Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' records.';
        SET @end_time = GETDATE()
        PRINT '>> ERP Product Category data is transformed and loaded successfully into silver.erp_px_cat_g1v2 Table.';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
        PRINT '';

        SET @batch_end_date = GETDATE()

        -----------------------------
        -- Completion Message
        -----------------------------
        PRINT '=========================================================';
        PRINT 'All Silver layer tables have been refreshed successfully!';
        PRINT 'Process Completed in: ' + CAST(DATEDIFF(second,@batch_start_date,@batch_end_date) AS NVARCHAR) + ' seconds';
        PRINT '=========================================================';

    END TRY
    BEGIN CATCH
        PRINT '=========================================================';
        PRINT 'ERROR OCCURED WHEN TRANSFORMING AND LOADING DATA INTO SILVER!!!';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR)
        PRINT '=========================================================';
    END CATCH
END
GO
