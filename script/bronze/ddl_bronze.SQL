/*
============================================
DDL SCRIPT:CREATE BRONZE TABELS
=============================================
Script purpose:
this script creates tabels in the 'bronze' schema, dropping existing tabels
if they already exists,
run this script to re-define the DDL structure of 'bronze' tabels
=============================================================================
*/

if object_id('bronze.crm_cust_info','u') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_material_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date
);

if object_id('bronze.crm_prd_info','u') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
prd_info int,
prd_key nvarchar(50),
prd_num nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt date,
prd_end_dt date
);

if object_id('bronze.crm_sales_details','u') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price int
);

if object_id('bronze.erp_cust_az12','u') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
CID nvarchar(50),
BDATE DATE,
GEN nvarchar(50)
);
	

if object_id('bronze.erp_loc_a101','u') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
	cid nvarchar(50),
	cntry nvarchar(50)

);


if object_id('bronze.erp_px_cat_g1v2','u') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;	
CREATE TABLE bronze.erp_px_cat_g1v2(
ID nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
MAINTENANCE nvarchar(50)
);
	



								

						
