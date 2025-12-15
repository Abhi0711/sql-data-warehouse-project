/*
====================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
====================================================================

Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'silver' schema tables from the 'bronze' schema.

Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;

====================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME
--CLEANING  AND INSERTING THE DATA FROM BRONZE TO SILVER 
BEGIN TRY
	SET @batch_start_time = GETDATE();
	PRINT'====================================================='
	PRINT'LOADING SILVER LAYER'
	PRINT'====================================================='

	PRINT'----------------------------------------------------------'
	PRINT'LOADING CRM TABLES'
	PRINT'----------------------------------------------------------'
	--LOADING SILVER.CRM_CUST_INFO
	SET @start_time = GETDATE();

	PRINT'>> TRUNCATING TABEL: SILVER.CRM_CUST_INFO';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT'>>INSERTING DATA INTO :SILVER.CRM_CUST_INFO'
	insert into silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)    
	select cst_id,
			cst_key,
			trim(cst_firstname) as cst_firstname ,
			trim(cst_lastname) as cst_lastname,
			CASE when upper(trim(cst_marital_status)) = 'S' then 'Single'
				when upper(trim(cst_marital_status)) = 'M' then 'Married'
			ELSE 'n/a'
			END cst_marital_status,

			CASE when upper(trim(cst_gndr)) = 'F' then 'Female'
				when upper(trim(cst_gndr)) = 'M' then 'Male'
			ELSE 'n/a'
			END cst_gndr,

			cst_create_date
	from(
	select 
	*,
	row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
	from bronze.crm_cust_info
	where cst_id is not null)t 
	where flag_last =1
	SET @end_time= GETDATE();
	PRINT'>>LOAD DURATION:'+ CAST(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar)+'seconds'
	print'>>-----------------------------------------------------'
	--<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	--CLEANING  AND INSERTING THE DATA FROM BRONZE TO SILVER 
	PRINT'>> TRUNCATING TABEL: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT'>>INSERTING DATA INTO :silver.crm_prd_info'

	insert into silver.crm_prd_info(
	prd_id ,
	cat_id,
	prd_key ,
	prd_num,
	prd_cost ,
	prd_line ,
	prd_start_dt,
	prd_end_dt  
	)
	SELECT
	prd_id,
	replace(substring(prd_key,1,5),'-','_')as cat_id,
	substring(prd_key,7,len(prd_key)) as prd_key,
	prd_num,
	ISNULL(prd_cost,0) AS prd_cost,
	case when upper(trim(prd_line)) = 'R' then 'Road'
		 when upper(trim(prd_line)) = 'M' then 'Mountain'
		 when upper(trim(prd_line)) = 'S' then 'Other sales'
		  when upper(trim(prd_line)) = 't' then 'Touring'
		  else 'N/A'
		end as prd_line,

	prd_start_dt,

	DATEADD(DAY,-1,lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)) as prd_end_date

	from bronze.crm_prd_info

	--<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	--CLEANING  AND INSERTING THE DATA FROM BRONZE TO SILVER 
	PRINT'>> TRUNCATING TABEL: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT'>>INSERTING DATA INTO :silver.crm_sales_details'

	INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price)
	SELECT sls_ord_num, 
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt) != 8 
		THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
		END AS sls_order_dt,

		sls_ship_dt,
		sls_due_dt,

		CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity* ABS(sls_price)
	THEN sls_quantity* ABS(sls_price)
	ELSE sls_sales
	END AS sls_sales,
	
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price<=0
	THEN sls_sales/ NULLIF(sls_quantity,0)
	else sls_price
	end AS sls_price
		FROM BRONZE.crm_sales_details

	--<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	--CLEANING  AND INSERTING THE DATA FROM BRONZE TO SILVER 
	PRINT'>> TRUNCATING TABEL: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT'>>INSERTING DATA INTO :silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12
	(CID,BDATE,GEN)

	SELECT 
	CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
	ELSE CID
	END AS CID,
	CASE WHEN BDATE > GETDATE() THEN NULL
	ELSE BDATE
	END AS BDATE,
	CASE 
		 WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'FEMALE'
		 WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'MALE'
		 ELSE 'N/A'
		 END AS GEN
	FROM bronze.erp_cust_az12;


	--<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
	--CLEANING  AND INSERTING THE DATA FROM BRONZE TO SILVER 
	PRINT'>> TRUNCATING TABEL: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT'>>INSERTING DATA INTO :silver.erp_loc_a101';

	INSERT INTO silver.erp_loc_a101(
	CID,CNTRY)

	select 
	REPLACE(cid,'-','') AS cid,
	case when TRIM(cntry) ='DE' THEN 'GERMANY'
		WHEN TRIM(cntry) IN ('US','USA') THEN 'N\A'
		ELSE TRIM(cntry)
		end as cntry
	from bronze.erp_loc_a101;

	--<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
	--CLEANING  AND INSERTING THE DATA FROM BRONZE TO SILVER 
	PRINT'>> TRUNCATING TABEL: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT'>>INSERTING DATA INTO :silver.erp_px_cat_g1v2';

	INSERT INTO silver.erp_px_cat_g1v2
	(ID,
	cat,
	subcat,
	MAINTENANCE)
	select ID,
	cat,subcat,MAINTENANCE
	from bronze.erp_px_cat_g1v2;

	SET @batch_end_time = GETDATE();
	PRINT'======================================'
	PRINT'LOADING SILVER LAYER COMPLETED';
	PRINT'TOTAL LOAD DURATION:'+ CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'SECONDS';
	PRINT'======================================'
	END TRY
	BEGIN CATCH
	PRINT'======================================';
	PRINT'ERROR OCCURED WHILE LOADING SILVER LAYER';
	PRINT'ERROR MESSAGE' + ERROR_MESSAGE();
	PRINT'ERROR MESSAGE'+CAST(ERROR_NUMBER() AS NVARCHAR) +'SECONDS';
	PRINT'ERROR MESSAGE'+CAST(ERROR_STATE() AS NVARCHAR);
	PRINT'======================================';
	END CATCH
END
