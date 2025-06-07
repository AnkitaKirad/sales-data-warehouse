/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze 
AS
BEGIN
	BEGIN TRY
	
		PRINT '================================';
		PRINT 'Loading Bronze layer';
		PRINT '================================';

		PRINT '---------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------------';

		PRINT '>> Truncating Table: [Bronze].[crm_cust_info]';
		Truncate table [Bronze].[crm_cust_info];

		PRINT '>> Inserting Table: [Bronze].[crm_cust_info]';
		BULK INSERT [Bronze].[crm_cust_info]
		FROM 'C:\Users\Niket Kirad\OneDrive\Desktop\Ankita\NewYork Trade Exchange Data Warehousing Project\Datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR='\n',
			TABLOCK
		);
		
		--Select * from [Bronze].[crm_cust_info];
		
		PRINT '>> Truncating Table: [Bronze].[crm_prd_info]';
		Truncate table [Bronze].[crm_prd_info];

		PRINT '>> Inserting Table: [Bronze].[crm_prd_info]';
		BULK INSERT [Bronze].[crm_prd_info]
		FROM 'C:\Users\Niket Kirad\OneDrive\Desktop\Ankita\NewYork Trade Exchange Data Warehousing Project\Datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR='\n',
			TABLOCK
		);
		
		--Select * from [Bronze].[crm_prd_info];
		PRINT '>> Truncating Table: [Bronze].[crm_sales_details]';
		Truncate table [Bronze].[crm_sales_details];

		PRINT '>> Inserting Table: [Bronze].[crm_sales_details]';
		BULK INSERT [Bronze].[crm_sales_details]
		FROM 'C:\Users\Niket Kirad\OneDrive\Desktop\Ankita\NewYork Trade Exchange Data Warehousing Project\Datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR='\n',
			TABLOCK
		);
		
		--Select * from [Bronze].[crm_sales_details];
		
		PRINT '---------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '---------------------------------';

		PRINT '>> Truncating Table: [Bronze].[erp_cust_az12]';
		Truncate table [Bronze].[erp_cust_az12];

		PRINT '>> Inserting Table: [Bronze].[erp_cust_az12]';
		BULK INSERT [Bronze].[erp_cust_az12]
		FROM 'C:\Users\Niket Kirad\OneDrive\Desktop\Ankita\NewYork Trade Exchange Data Warehousing Project\Datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR='\n',
			TABLOCK
		);
		
		--Select * from [Bronze].[erp_cust_az12];

		PRINT '>> Truncating Table: [Bronze].[erp_loc_a101]';
		Truncate table [Bronze].[erp_loc_a101];

		PRINT '>> Inserting Table: [Bronze].[erp_loc_a101]';
		BULK INSERT [Bronze].[erp_loc_a101]
		FROM 'C:\Users\Niket Kirad\OneDrive\Desktop\Ankita\NewYork Trade Exchange Data Warehousing Project\Datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR='\n',
			TABLOCK
		);
		
		--Select * from [Bronze].[erp_loc_a101];
		
		PRINT '>> Truncating Table: [Bronze].[erp_px_cat_g1v2]';
		Truncate table [Bronze].[erp_px_cat_g1v2];

		PRINT '>> Inserting Table: [Bronze].[erp_px_cat_g1v2]';
		BULK INSERT [Bronze].[erp_px_cat_g1v2]
		FROM 'C:\Users\Niket Kirad\OneDrive\Desktop\Ankita\NewYork Trade Exchange Data Warehousing Project\Datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR='\n',
			TABLOCK
		);
		
		--Select * from [Bronze].[erp_px_cat_g1v2];
	END TRY
	BEGIN CATCH
		PRINT '=====================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=====================================';
	END CATCH
END