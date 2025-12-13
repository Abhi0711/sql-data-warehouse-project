/*
========================================================================
stored procedure: Load bronze Layer (source-> bronze)
========================================================================
script purpose:
this stored procedure loads data into the 'bronze' schema from external CSV file.
Here is the extracted text from the image you provided:

It performs the following actions:
 - Truncates the bronze tables before loading data.
 - Uses the BULK INSERT command to load data from CSV files to bronze tables.

Parameters:
 None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
 EXEC bronze.load_bronze;
================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze as
BEGIN

DECLARE @start_time datetime, @end_time datetime, @batch_sttime datetime, @batch_edtime datetime
	BEGIN TRY 

PRINT'=====================================';
PRINT'LOADING BRONZELAYER'
PRINT'=====================================';

PRINT'---------------------------------';
PRINT'LOADING CRM TABEL'
PRINT'---------------------------------';
SET @batch_sttime = GETDATE();
SET @start_time = GETDATE();
PRINT'>>TRUNCATING TABLE: bronze.crm_cust_info';
Truncate table bronze.crm_cust_info;

PRINT'>> INSERT DATA INTO TABEL: bronze.crm_cust_info';
BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\XPB2KOR\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with (
	firstrow = 2,
	fieldterminator =',',
	tablock
	);
	SET @end_time = GETDATE();
	PRINT'LOAD DURATION'+ CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) +'seconds';



PRINT'---------------------------------'
PRINT'LOADING CRM TABEL'
PRINT'---------------------------------'
--file crd_prd
SET @start_time = GETDATE();
Truncate table bronze.crm_prd_info;
BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\XPB2KOR\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with (
	firstrow = 2,
	fieldterminator =',',
	tablock
	);
	SET @end_time = GETDATE();
	PRINT'LOAD DURATION'+ CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) +'seconds';


--file sales_details
SET @start_time = GETDATE();
Truncate table bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\XPB2KOR\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with (
	firstrow = 2,
	fieldterminator =',',
	tablock
	);
	SET @end_time = GETDATE();
	PRINT'LOAD DURATION'+ CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) +'seconds';




--file for ERP-cust
SET @start_time = GETDATE();
Truncate table bronze.erp_cust_az12;
BULK INSERT bronze.erp_cust_az12
FROM 'C:\Users\XPB2KOR\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with (
	firstrow = 2,
	fieldterminator =',',
	tablock
	); 
	SET @end_time = GETDATE();
	PRINT'LOAD DURATION'+ CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) +'seconds';

--file erp_loc_a101
SET @start_time = GETDATE();
Truncate table bronze.erp_loc_a101;
BULK insert bronze.erp_loc_a101
FROM'C:\Users\XPB2KOR\Desktop\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
with (
	firstrow=2,
	fieldterminator=',',
	tablock
	);
	SET @end_time = GETDATE();
	PRINT'LOAD DURATION ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) +'seconds';

--file erp_px_cat_g1v2
SET @start_time = GETDATE();
Truncate table bronze.erp_px_cat_g1v2;
BULK insert bronze.erp_px_cat_g1v2
FROM'C:\Users\XPB2KOR\Desktop\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
with (
	firstrow=2,
	fieldterminator=',',
	tablock
	);
	SET @end_time = GETDATE();
	PRINT'LOAD DURATION'+ CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) +'seconds';
	SET @batch_edtime = GETDATE();
	PRINT'TOTAL LOGGING DATA IS ' +CAST(DATEDIFF(SECOND,@batch_sttime,@batch_edtime)AS NVARCHAR) +'SECONDS'
	END TRY
	BEGIN CATCH
	PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER'
	END CATCH
END
