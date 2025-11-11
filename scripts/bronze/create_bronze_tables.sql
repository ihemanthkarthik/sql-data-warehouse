/***************************************************************************************************
File Name   : create_bronze_tables.sql
Database    : DataWarehouse
Schema      : bronze
Author      : Hemanth Kumar Karthikeyan
Created On  : 11-11-2025

Description :
This script initializes the Bronze Layer of the Data Warehouse by creating raw ingestion tables 
for CRM and ERP source systems. It ensures that any existing versions of the tables are dropped 
before creation to maintain a clean structure for fresh data loads.

Tables Created :
1. bronze.crm_cust_info       – Stores customer master data from CRM
2. bronze.crm_prd_info        – Stores product master data from CRM
3. bronze.crm_sales_details   – Stores sales transaction details from CRM
4. bronze.erp_cust_az12       – Stores ERP customer details
5. bronze.erp_loc_a101        – Stores ERP customer location information
6. bronze.erp_px_cat_g1v2     – Stores ERP product category and subcategory details

Usage :
Run this script before the initial data ingestion process to set up the required raw (bronze) tables.

Notes :
- All tables are recreated each time this script runs.
- Data types are chosen to match raw source structure for staging purposes.
***************************************************************************************************/

USE DataWarehouse;
GO

-- CREATE TABLE FOR CRM Files
-- CHECKING IF TABLE crm_cust_info exists and dropping table if exists
If OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;

-- CREATE TABLE crm_cust_info
CREATE TABLE bronze.crm_cust_info(
	cst_id				INT,
	cst_key				NVARCHAR(50),
	cst_firstname		NVARCHAR(50),
	cst_lastname		NVARCHAR(50),
	cst_marital_status	NVARCHAR(50),
	cst_gndr			NVARCHAR(50),
	cst_create_date		DATE
);

-- CHECKING IF TABLE crm_cust_info exists and dropping table if exists
If OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;

-- CREATE TABLE crm_prd_info
CREATE TABLE bronze.crm_prd_info(
	prd_id			INT,
	prd_key			NVARCHAR(50),
	prd_nm			NVARCHAR(50),
	prd_cost		INT,
	prd_line		NVARCHAR(50),
	prd_start_dt	DATETIME,
	prd_end_dt		DATETIME
);

-- CHECKING IF TABLE crm_cust_info exists and dropping table if exists
If OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;

-- CREATE TABLE crm_sales_details
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num		NVARCHAR(50),
	sls_prd_key		NVARCHAR(50),
	sls_cust_id		INT,
	sls_order_dt	INT,
	sls_ship_dt		INT,
	sls_due_dt		INT,
	sls_sales		INT,
	sls_quantity	INT,
	sls_price		INT
);

-- CREATE TABLE FOR ERP Files
-- CHECKING IF TABLE crm_cust_info exists and dropping table if exists
If OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;

-- CREATE TABLE erp_cust_az12
CREATE TABLE bronze.erp_cust_az12(
	cid		NVARCHAR(50),
	bdate	DATE,
	gen		NVARCHAR(50)
);

-- CHECKING IF TABLE crm_cust_info exists and dropping table if exists
If OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;

-- CREATE TABLE erp_loc_a101
CREATE TABLE bronze.erp_loc_a101(
	cid		NVARCHAR(50),
	country NVARCHAR(50)
);

-- CHECKING IF TABLE crm_cust_info exists and dropping table if exists
If OBJECT_ID('bronze.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;

-- CREATE TABLE erp_px_cat_g1v2
CREATE TABLE bronze.erp_px_cat_g1v2(
	id			NVARCHAR(50),
	cat			NVARCHAR(50),
	subcat		NVARCHAR(50),
	maintenance NVARCHAR(50),
);
