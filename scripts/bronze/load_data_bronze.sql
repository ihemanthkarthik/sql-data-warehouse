/***************************************************************************************************
File Name   : load_data_bronze.sql
Stored Proc : [bronze].[load_data_bronze]
Database    : DataWarehouse
Author      : Hemanth Kumar Karthikeyan
Created On  : 11-Nov-2025
Version     : 1.0

Description :
This stored procedure performs the initial data ingestion into the Bronze Layer of the Data Warehouse.
It automates the extraction and loading of raw CSV source files from the CRM and ERP systems into 
their respective staging tables in the [bronze] schema.

The process includes:
1. Truncating existing data in bronze tables to ensure a fresh load.
2. Bulk inserting data from CSV files located in the local directory paths.
3. Printing detailed step-by-step log messages to monitor progress and load times.
4. Handling and reporting errors gracefully using TRY-CATCH blocks.

Tables Loaded :
----------------
CRM Source:
    1. bronze.crm_cust_info       – Customer master data
    2. bronze.crm_prd_info        – Product master data
    3. bronze.crm_sales_details   – Sales transaction data

ERP Source:
    4. bronze.erp_cust_az12       – Customer information
    5. bronze.erp_loc_a101        – Customer location details
    6. bronze.erp_px_cat_g1v2     – Product category and subcategory information

Key Features :
---------------
- Uses BULK INSERT for fast, efficient data loading.
- Tracks start/end timestamps and calculates load duration for each dataset.
- Implements error handling using TRY-CATCH to ensure controlled failure management.
- Prints load summary for quick verification.

Usage :
--------
EXEC bronze.load_data_bronze;

Notes :
--------
- Ensure CSV file paths in the BULK INSERT statements are accessible and correct.
- The destination tables must exist before running the procedure.
- Designed for Windows file system paths; adjust file locations as needed for deployment.
***************************************************************************************************/

USE [DataWarehouse]
GO

/****** Object:  StoredProcedure [bronze].[load_data_bronze]    Script Date: 11-11-2025 12:37:35 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER PROCEDURE [bronze].[load_data_bronze]
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_date DATETIME, @batch_end_date DATETIME
    BEGIN TRY
        SET NOCOUNT ON;

        SET @batch_start_date = GETDATE();

        PRINT '=========================================================';
        PRINT 'Starting Bronze Layer Data Load Process...';
        PRINT '=========================================================';
        PRINT '';

        -----------------------------
        -- Load CRM Customer Info
        -----------------------------

        PRINT '-----------------------------'
        PRINT '--- Load CRM Tables ---'
        PRINT '-----------------------------'
        PRINT '';

        SET @start_time = GETDATE()
        PRINT 'Step 1: Loading CRM Customer Info data...';
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT '>> Table bronze.crm_cust_info truncated successfully.';

        PRINT '>> Inserting Data into bronze.crm_cust_info Table...';
        BULK INSERT bronze.crm_cust_info 
        FROM 'D:\Learning\Data With Baraa\Data Engineering Project\sql-data-warehouse\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        PRINT '>> Total Records Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' records.';
        SET @end_time = GETDATE()
        PRINT '>> CRM Customer Info data loaded successfully into bronze.crm_cust_info Table.';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
        PRINT '';

        -----------------------------
        -- Load CRM Product Info
        -----------------------------

        SET @start_time = GETDATE()
        PRINT 'Step 2: Loading CRM Product Info data...';
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT '>> Table bronze.crm_prd_info truncated successfully.';

        PRINT '>> Inserting Data into bronze.crm_prd_info Table...';
        BULK INSERT bronze.crm_prd_info 
        FROM 'D:\Learning\Data With Baraa\Data Engineering Project\sql-data-warehouse\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        PRINT '>> Total Records Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' records.';
        SET @end_time = GETDATE()
        PRINT '>> CRM Product Info data loaded successfully into bronze.crm_prd_info Table.';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
        PRINT '';

        -----------------------------
        -- Load CRM Sales Details
        -----------------------------

        SET @start_time = GETDATE()
        PRINT 'Step 3: Loading CRM Sales Details data...';
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT '>> Table bronze.crm_sales_details truncated successfully.';

        PRINT '>> Inserting Data into bronze.crm_sales_details Table...';
        BULK INSERT bronze.crm_sales_details 
        FROM 'D:\Learning\Data With Baraa\Data Engineering Project\sql-data-warehouse\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        PRINT '>> Total Records Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' records.';
        SET @end_time = GETDATE()
        PRINT '>> CRM Sales Details data loaded successfully into bronze.crm_sales_details Table.';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
        PRINT '';

        -----------------------------
        -- Load ERP Customer Data
        -----------------------------

        PRINT '-----------------------------'
        PRINT '--- Load ERP Tables ---'
        PRINT '-----------------------------'
        PRINT '';

        SET @start_time = GETDATE()
        PRINT 'Step 4: Loading ERP Customer (CUST_AZ12) data...';
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT '>> Table bronze.erp_cust_az12 truncated successfully.';

        PRINT '>> Inserting Data into bronze.erp_cust_az12 Table...';
        BULK INSERT bronze.erp_cust_az12 
        FROM 'D:\Learning\Data With Baraa\Data Engineering Project\sql-data-warehouse\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        PRINT '>> Total Records Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' records.';
        SET @end_time = GETDATE()
        PRINT '>> ERP Customer data loaded successfully into bronze.erp_cust_az12 Table.';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
        PRINT '';

        -----------------------------
        -- Load ERP Location Data
        -----------------------------

        SET @start_time = GETDATE()
        PRINT 'Step 5: Loading ERP Location (LOC_A101) data...';
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT '>> Table bronze.erp_loc_a101 table successfully.';

        PRINT '>> Inserting Data into bronze.erp_loc_a101 Table...';
        BULK INSERT bronze.erp_loc_a101 
        FROM 'D:\Learning\Data With Baraa\Data Engineering Project\sql-data-warehouse\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        PRINT '>> Total Records Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' records.';
        SET @end_time = GETDATE()
        PRINT '>> ERP Location data loaded successfully into bronze.erp_loc_a101 Table.';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
        PRINT '';

        -----------------------------------
        -- Load ERP Product Category Data
        -----------------------------------

        SET @start_time = GETDATE()
        PRINT 'Step 6: Loading ERP Product Category (PX_CAT_G1V2) data...';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT '>> Table bronze.erp_px_cat_g1v2 truncated successfully.';

        PRINT '>> Inserting Data into bronze.erp_px_cat_g1v2 Table...';
        BULK INSERT bronze.erp_px_cat_g1v2 
        FROM 'D:\Learning\Data With Baraa\Data Engineering Project\sql-data-warehouse\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        PRINT '>> Total Records Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' records.';
        SET @end_time = GETDATE()
        PRINT '>> ERP Product Category data loaded successfully into bronze.erp_px_cat_g1v2 Table.';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
        PRINT '';

        SET @batch_end_date = GETDATE()

        -----------------------------
        -- Completion Message
        -----------------------------
        PRINT '=========================================================';
        PRINT 'All Bronze layer tables have been refreshed successfully!';
        PRINT 'Process Completed in: ' + CAST(DATEDIFF(second,@batch_start_date,@batch_end_date) AS NVARCHAR) + ' seconds';
        PRINT '=========================================================';

    END TRY
    BEGIN CATCH
        PRINT '=========================================================';
        PRINT 'ERROR OCCURED WHEN LOADING DATA INTO BRONZE!!!';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR)
        PRINT '=========================================================';
    END CATCH
END
GO
