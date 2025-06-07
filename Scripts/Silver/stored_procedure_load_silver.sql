/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
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
===============================================================================
*/

CREATE OR ALTER PROCEDURE Silver.load_silver
AS
BEGIN
	BEGIN TRY
		------------------------------------------------------------------------------------------
		--Cleaing the data in [Bronze].[crm_cust_info] and loading into [Silver].[crm_cust_info]
		-------------------------------------------------------------------------------------------
		PRINT '>> Truncating Table: [Silver].[crm_cust_info]'
		TRUNCATE TABLE [Silver].[crm_cust_info];
		
		--Elemenating the duplicate cst_id based on selecting the latest record
		--Removing spaces from varchar columns firstname,lastname
		--Standarizing and bringing consistency for cst_marital_status,cst_gndr column
		--Handling Missing value replacing NULL with default value(NA)
		--Inserting data into [Silver].[crm_cust_info]
		PRINT '>> Inserting Table: [Silver].[crm_cust_info]'
		
		INSERT INTO [Silver].[crm_cust_info]
		(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		SELECT
			sub.cst_id,
			sub.cst_key,
			TRIM(sub.cst_firstname),
			TRIM(sub.cst_lastname),
			CASE 
				WHEN Upper(TRIM(sub.cst_marital_status)) = 'S'
					THEN 'Single'
				WHEN Upper(TRIM(sub.cst_marital_status)) = 'M'
					THEN 'Married'
				ELSE
					'NA'
			END AS cst_marital_status,
			CASE 
				WHEN Upper(TRIM(sub.cst_gndr)) = 'F'
					THEN 'Female'
				WHEN Upper(TRIM(sub.cst_gndr)) = 'M'
					THEN 'Male'
				ELSE
					'NA'
			END AS cst_gndr,
			sub.cst_create_date
		FROM
		(
			SELECT
				*,
				ROW_NUMBER() over(Partition by cst_id order by cst_create_date desc) AS [latest_flag]
			FROM [Bronze].[crm_cust_info]
			WHERE cst_id is NOT NULL
		)sub
		where sub.latest_flag = 1;
		
		------------------------------------------------------------------------------------------
		--Cleaing the data in [Bronze].[crm_cust_info] and loading into [Silver].[crm_cust_info]
		-------------------------------------------------------------------------------------------
		
		PRINT '>> Truncating Table: [Silver].[crm_prd_info]'
		TRUNCATE TABLE [Silver].[crm_prd_info];
		--Product key column has lot of information stored so Extracting the informtion from that stirng
		--Hadling Null values in cost
		--Standarizing and bringing consistency for prd_line column
		--Casting the start date and end date as DATE
		--Fixing the issue with end date column 
		
		PRINT '>> Inserting Table: [Silver].[crm_prd_info]'
		INSERT INTO [Silver].[crm_prd_info]
		(
			prd_id,
			cat_id,
		    prd_key,
		    prd_nm,
		    prd_cost,
		    prd_line,
		    prd_start_dt,
		    prd_end_dt  
		)
		SELECT
			p.prd_id,
			Replace(SUBSTRING(p.prd_key,1,5),'-','_') AS cat_id,
			SUBSTRING(p.prd_key,7,LEN(p.prd_key)) AS prd_key,
			p.prd_nm,
			ISNULL(p.prd_cost,0) AS prd_cost,
			CASE Upper(TRIM(p.prd_line))
				WHEN 'M'
					THEN 'Mountain'
				WHEN 'R'
					THEN 'Road'
				WHEN 'S'
					THEN 'Other Sales'
				WHEN 'T'
					THEN 'Touring'
				ELSE
					'NA'
			END AS prd_line,
			CAST(p.prd_start_dt AS DATE) AS prd_start_dt,
			CAST((LEAD(p.prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt))-1 AS DATE) AS prd_end_dt
		FROM [Bronze].[crm_prd_info] p;
		
		------------------------------------------------------------------------------------------
		--Cleaing the data in [Bronze].[crm_sales_details] and loading into [Silver].[crm_sales_details]
		-------------------------------------------------------------------------------------------
		
		PRINT '>> Truncating Table: [Silver].[crm_sales_details]'
		TRUNCATE TABLE [Silver].[crm_sales_details];
		--Transforing the order date column from int to date
		--Enhancing the sales,quantity and price
		--checking the sales = quantity * price
		
		PRINT '>> Inserting Table: [Silver].[crm_sales_details]'
		INSERT INTO [Silver].[crm_sales_details]
		(
			sls_ord_num ,
		    sls_prd_key,
		    sls_cust_id,
		    sls_order_dt,
		    sls_ship_dt,
		    sls_due_dt,
		    sls_sales,
		    sls_quantity,
		    sls_price
		)
		SELECT
			s.sls_ord_num,
			s.sls_prd_key,
			s.sls_cust_id,
			CASE 
				WHEN s.sls_order_dt = 0 OR LEN(s.sls_order_dt) != 8 
					THEN NULL
				ELSE CAST(CAST(s.sls_order_dt AS varchar) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN s.sls_ship_dt = 0 OR LEN(s.sls_ship_dt) != 8 
					THEN NULL
				ELSE CAST(CAST(s.sls_ship_dt AS varchar) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN s.sls_due_dt = 0 OR LEN(s.sls_due_dt) != 8 
					THEN NULL
				ELSE CAST(CAST(s.sls_due_dt AS varchar) AS DATE)
			END AS sls_due_dt,
			CASE	
				WHEN s.sls_sales IS NULL OR s.sls_sales <= 0 OR s.sls_sales != s.sls_quantity * ABS(s.sls_price)
					THEN s.sls_quantity * ABS(s.sls_price)
				ELSE s.sls_sales
			END AS sls_sales,
			s.sls_quantity,
			CASE
				WHEN s.sls_price IS NULL OR s.sls_price <= 0
					THEN s.sls_sales / NULLIF(s.sls_quantity,0)
				ELSE s.sls_price
			END AS sls_price
		FROM [Bronze].[crm_sales_details] s;
		
		------------------------------------------------------------------------------------------
		--Cleaing the data in [Bronze].[erp_cust_az12] and loading into [Silver].[erp_cust_az12]
		-------------------------------------------------------------------------------------------
		
		PRINT '>> Truncating Table: [Silver].[erp_cust_az12]'
		TRUNCATE TABLE [Silver].[erp_cust_az12];
		--Extracting the Valid string from cid
		--Validating the  bday
		--Standardization and consistency in gender column
		
		PRINT '>> Inserting Table: [Silver].[erp_cust_az12]'
		INSERT INTO [Silver].[erp_cust_az12]
		(
			cid,
		    bdate,
		    gen
		)
		SELECT 
			CASE 
				WHEN ec.cid like 'NAS%' 
					THEN SUBSTRING(cid,4,LEN(cid))
				ELSE	
					cid
			END AS cid,
			CASE 
				WHEN ec.bdate > GETDATE() 
					THEN NULL
				ELSE	
					ec.bdate
			END AS bdate,
			CASE 
				WHEN Upper(TRIM(ec.gen)) in ('F','Female')
					THEN 'Female'
				WHEN Upper(TRIM(ec.gen)) in ('M','Male')
					THEN 'Male'
				ELSE
					'NA'
			END AS gen
		FROM [Bronze].[erp_cust_az12] ec;
		
		------------------------------------------------------------------------------------------
		--Cleaing the data in [Bronze].[erp_loc_a101] and loading into [Silver].[erp_loc_a101]
		-------------------------------------------------------------------------------------------
		
		PRINT '>> Truncating Table: [Silver].[erp_loc_a101]'
		TRUNCATE TABLE [Silver].[erp_loc_a101];
		--Replacing - from cid to match it with crm_cust_info for joining
		
		PRINT '>> Inserting Table: [Silver].[erp_loc_a101]'
		--Standardization of Country
		INSERT INTO [Silver].[erp_loc_a101]
		(
			cid,
			cntry
		)
		Select
			REPLACE(be.cid,'-','') cid,
			CASE
				WHEN UPPER(TRIM(be.cntry)) = 'DE' 
					THEN 'Germany'
				WHEN UPPER(TRIM(be.cntry)) IN ('US','USA') 
					THEN 'United States'
				WHEN UPPER(TRIM(be.cntry)) = '' OR be.cntry is NULL
					THEN 'NA'
				ELSE TRIM(be.cntry)
			END AS cntry
		from [Bronze].[erp_loc_a101] be;
		
		------------------------------------------------------------------------------------------
		--Cleaing the data in [Bronze].[erp_px_cat_g1v2] and loading into [Silver].[erp_px_cat_g1v2]
		-------------------------------------------------------------------------------------------
		PRINT '>> Truncating Table: [Silver].[erp_px_cat_g1v2]'
		TRUNCATE TABLE [Silver].[erp_px_cat_g1v2];
		PRINT '>> Inserting Table: [Silver].[erp_px_cat_g1v2]'
		INSERT INTO [Silver].[erp_px_cat_g1v2]
		(
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM [Bronze].[erp_px_cat_g1v2]
	END TRY
	BEGIN CATCH
		PRINT '==============================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==============================================='
	END CATCH
END