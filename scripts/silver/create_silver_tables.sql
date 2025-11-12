/***************************************************************************************************
File Name   : create_silver_tables.sql
Database    : DataWarehouse
Schema      : silver
Author      : Hemanth Kumar Karthikeyan
Created On  : 11-Nov-2025
Version     : 1.0

Description :
This script creates the Silver-layer tables used for cleaned, standardized and analysis-ready staging
of CRM and ERP data. The Silver layer is populated from the Bronze layer after applying data quality
rules and business transformations. Each Silver table includes an audit column (dwh_create_date) that
captures the timestamp when the row was inserted/loaded into the Silver layer.

Scope:
- Drops and recreates these tables in the [silver] schema:
    * silver.crm_cust_info
    * silver.crm_prd_info
    * silver.crm_sales_details
    * silver.erp_cust_az12
    * silver.erp_loc_a101
    * silver.erp_px_cat_g1v2

Purpose of this comment block:
- Document what changed in the Silver DDL compared to the Bronze DDL.
- Provide rationale for each change, migration notes and production recommendations.

-------------------------
Summary of changes vs Bronze DDL
(Compare this Silver DDL against the original Bronze-layer DDL used for raw ingestion)
-------------------------

1) Audit column added
   - Change: Added `dwh_create_date DATETIME2 DEFAULT GETDATE()` to all Silver tables.
   - Reason: Track load timestamp / lineage for ETL auditing and debugging.

2) Data type normalizations & tightening
   - crm_prd_info:
     * Bronze: prd_start_dt DATETIME, prd_end_dt DATETIME
     * Silver: prd_start_dt DATE, prd_end_dt DATE
     * Reason: Only date granularity required for analytics; removes time component and reduces storage.
   - crm_sales_details:
     * Bronze: sls_order_dt, sls_ship_dt, sls_due_dt were INT (YYYYMMDD-like)
     * Silver: those columns are DATE
     * Reason: Convert numeric date representation into native DATE for correct date operations and indexing.
   - Currency/amount columns:
     * Bronze used INT for prd_cost, sls_sales, sls_price.
     * Recommendation: consider DECIMAL(18,2) if fractional currency or better precision is needed (see production recommendations below).

3) Structural / semantic changes
   - crm_prd_info:
     * Added `prd_cat_id NVARCHAR(50)` in Silver (derived from prd_key in transformation logic).
     * Retained `prd_key` but now expected to be the cleaned/normalized key (transformation logic extracts the real key).
     * Reason: Separate catalog-level id vs product instance key simplifies joins and analytic grouping.
   - crm_cust_info:
     * Schema preserved but values are expected cleaned/normalized (e.g., trimmed names, expanded codes).
   - erp tables:
     * Column names preserved but values are expected normalized (e.g., country standardized, CID cleaned).

4) Modeling intent vs Bronze (raw)
   - Bronze layer = raw ingestion, keep original formats and column names as-is to preserve source fidelity.
   - Silver layer = cleaned, canonicalized formats intended for downstream joins/analytics.
   - Silver intentionally contains defaults/audit info and tighter datatypes to enable reliable transformations and
     easier debugging in the Gold/business layer.

5) Default behavior and constraints (what is present and what is intentionally omitted)
   - Present: dwh_create_date default to GETDATE() for line-level audit.
   - Intentionally omitted (in this DDL): Primary keys, NOT NULL constraints, indexes, and FK constraints.
     * Rationale: Silver here is staged as a cleaned landing zone. Enforcing some constraints at Silver can be
       applied later (or added after an initial validation load). However, production readiness suggestions follow.

-------------------------
Migration & operational notes
-------------------------
- Run order:
  1. Ensure `schema silver` exists: `IF SCHEMA_ID('silver') IS NULL EXEC('CREATE SCHEMA silver');`
  2. Create / recreate Silver tables (this script).
  3. Execute transformation job that moves data from Bronze → Silver (script or stored proc).
- Data validation:
  * Validate date conversions (INT → DATE) for all rows before enabling NOT NULL / FK constraints.
  * Verify `prd_key` parsing logic (prd_cat_id derivation) against actual data patterns to avoid key truncation.
- File paths / permissions:
  * Not applicable to DDL, but the ETL process that fills Silver must have necessary read access to Bronze or source files.
- Backups:
  * Because this script drops tables, ensure you have backups or use CREATE TABLE IF NOT EXISTS + ALTER for production migrations.

-------------------------
Production recommendations (high-value)
-------------------------
1. Primary Keys / Unique Constraints
   - Add PKs once you confirm uniqueness:
     * `silver.crm_cust_info` → `PRIMARY KEY (cst_id)` if cst_id is unique and stable.
     * `silver.crm_prd_info` → `PRIMARY KEY (prd_id)` or `UNIQUE(prd_key)`.
     * `silver.crm_sales_details` → add surrogate key (IDENTITY) or composite key based on business rules.
2. Data types for money/amounts
   - Convert amount columns (prd_cost, sls_sales, sls_price) to `DECIMAL(18,2)` if currency/fractions exist.
3. NOT NULL where appropriate
   - Set NOT NULL for mandatory business fields after validating source completeness (e.g., identifiers).
4. Indexing
   - Create nonclustered indexes on join/filter columns (cst_key, sls_prd_key, cid) used by frequent queries.
5. Referential integrity
   - Optionally add FKs to enforce integrity (e.g., sales.sls_cust_id → crm_cust_info.cst_id) if Silver is treated as canonical.
6. Partitioning & maintenance
   - For large fact tables (sales), consider partitioning by date (sls_order_dt) and implement rolling-window maintenance.
7. Audit & lineage
   - Consider adding `dwh_loaded_by`, `source_file_name`, `batch_id` if you need richer lineage tracking.
8. Security & permissions
   - Grant only necessary DML on these tables to ETL service accounts; keep DROP/ALTER restricted.
9. Change management
   - Avoid `DROP TABLE` in production without approvals. Use ALTER/CREATE OR ALTER or migration scripts (idempotent changes).
10. CI/CD
    - Put these DDLs under source control and automate deployment (e.g., with a pipeline that performs schema drift checks).

-------------------------
Change Log (explicit lines)
-------------------------
- 11-Nov-2025  Version 1.0  - Initial Silver DDL release.
    * Added audit field `dwh_create_date` to all tables.
    * Converted CRM sales date columns from INT → DATE.
    * Converted product date columns from DATETIME → DATE.
    * Added `prd_cat_id` column to silver.crm_prd_info (expected to be derived during transform).
    * Adjusted `crm_prd_info` column ordering and names to reflect cleaned data model.
    * Left monetary fields as INT in DDL but recommended converting to DECIMAL in production.

***************************************************************************************************/
USE DataWarehouse;
GO

-- CREATE TABLE FOR CRM Files
-- CHECKING IF TABLE crm_cust_info exists and dropping table if exists
If OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;

-- CREATE TABLE crm_cust_info
CREATE TABLE silver.crm_cust_info(
	cst_id				INT,
	cst_key				NVARCHAR(50),
	cst_firstname		NVARCHAR(50),
	cst_lastname		NVARCHAR(50),
	cst_marital_status	NVARCHAR(50),
	cst_gndr			NVARCHAR(50),
	cst_create_date		DATE,
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
);

-- CHECKING IF TABLE crm_cust_info exists and dropping table if exists
If OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;

-- CREATE TABLE crm_prd_info
CREATE TABLE silver.crm_prd_info(
	prd_id			INT,
	prd_cat_id		NVARCHAR(50),
	prd_key			NVARCHAR(50),
	prd_nm			NVARCHAR(50),
	prd_cost		INT,
	prd_line		NVARCHAR(50),
	prd_start_dt	DATE,
	prd_end_dt		DATE,
	dwh_create_date	DATETIME2 DEFAULT GETDATE()
);

-- CHECKING IF TABLE crm_cust_info exists and dropping table if exists
If OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;

-- CREATE TABLE crm_sales_details
CREATE TABLE silver.crm_sales_details(
	sls_ord_num		NVARCHAR(50),
	sls_prd_key		NVARCHAR(50),
	sls_cust_id		INT,
	sls_order_dt	DATE,
	sls_ship_dt		DATE,
	sls_due_dt		DATE,
	sls_sales		INT,
	sls_quantity	INT,
	sls_price		INT,
	dwh_create_date	DATETIME2 DEFAULT GETDATE()
);

-- CREATE TABLE FOR ERP Files
-- CHECKING IF TABLE crm_cust_info exists and dropping table if exists
If OBJECT_ID('silver.erp_cust_az12','U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;

-- CREATE TABLE erp_cust_az12
CREATE TABLE silver.erp_cust_az12(
	cid				NVARCHAR(50),
	bdate			DATE,
	gen				NVARCHAR(50),
	dwh_create_date	DATETIME2 DEFAULT GETDATE()
);

-- CHECKING IF TABLE crm_cust_info exists and dropping table if exists
If OBJECT_ID('silver.erp_loc_a101','U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;

-- CREATE TABLE erp_loc_a101
CREATE TABLE silver.erp_loc_a101(
	cid				NVARCHAR(50),
	country 		NVARCHAR(50),
	dwh_create_date	DATETIME2 DEFAULT GETDATE()
);

-- CHECKING IF TABLE crm_cust_info exists and dropping table if exists
If OBJECT_ID('silver.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;

-- CREATE TABLE erp_px_cat_g1v2
CREATE TABLE silver.erp_px_cat_g1v2(
	id				NVARCHAR(50),
	cat				NVARCHAR(50),
	subcat			NVARCHAR(50),
	maintenance 	NVARCHAR(50),
	dwh_create_date	DATETIME2 DEFAULT GETDATE()
);
