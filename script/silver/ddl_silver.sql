/*DDL Script: Create Silver Tables

Script Purpose:
This script creates tables in the 'silver' schema, dropping existing tables
if they already exist.
Run this script to re-define the DDL structure of 'bronze' tables
*/
--CREATING A TABEL WITH SILVER SCHEMA

if object_id('silver.crm_cust_info','u') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
GO
CREATE TABLE silver.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date,
dwh_create_date datetime2 default getdate()
);
GO
  
if object_id('silver.crm_prd_info','u') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
prd_id int,
cat_id nvarchar(50),
prd_key nvarchar(50),
prd_num nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt date,
prd_end_dt date,
--dwh_create_date datetime2 default getdate()
);
GO
if object_id('silver.crm_sales_details','u') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date datetime2 default getdate()
);
GO
if object_id('silver.erp_cust_az12','u') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
CID nvarchar(50),
BDATE DATE,
GEN nvarchar(50),
dwh_create_date datetime2 default getdate()
);
	
GO
if object_id('silver.erp_loc_a101','u') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
	cid nvarchar(50),
	cntry nvarchar(50),
dwh_create_date datetime2 default getdate()

);
GO

if object_id('silver.erp_px_cat_g1v2','u') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;	
CREATE TABLE silver.erp_px_cat_g1v2(
ID nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
MAINTENANCE nvarchar(50),
dwh_create_date datetime2 default getdate()
);
	
