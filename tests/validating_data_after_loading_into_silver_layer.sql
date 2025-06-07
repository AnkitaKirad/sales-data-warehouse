/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

--------------------------------------------------------------------------
--Checking the data quality in [Silver].[crm_cust_info]
-------------------------------------------------------------------------

SELECT
	*
FROM [Silver].[crm_cust_info];

--checking for NULL and Duplicate value for primary key(cst_id)
--Should return no records
SELECT 
	c.cst_id,
	count(*)
FROM [Silver].[crm_cust_info] c
group by c.cst_id
having count(*) > 1 or c.cst_id is NULL

--Checking for Unwanted Spaces
--Should return no records
SELECT
	c.cst_lastname
FROM [Silver].[crm_cust_info] c
where c.cst_lastname != TRIM(c.cst_lastname)

SELECT
	c.cst_firstname
FROM [Silver].[crm_cust_info] c
where c.cst_firstname != TRIM(c.cst_firstname)

SELECT
	c.cst_gndr
FROM [Silver].[crm_cust_info] c
where c.cst_gndr != TRIM(c.cst_gndr)

SELECT
	c.cst_marital_status
FROM [Silver].[crm_cust_info] c
where c.cst_marital_status != TRIM(c.cst_marital_status)

--Data Standardization & Consistency
--Should return changed and updated records
SELECT DISTINCT c.cst_gndr
FROM Silver.crm_cust_info c

SELECT DISTINCT c.cst_marital_status
FROM Silver.crm_cust_info c

--------------------------------------------------------------------------
--Checking the data quality in [Silver].[crm_prd_info]
-------------------------------------------------------------------------

--Check if dwh_created_date is added
SELECT * FROM [Silver].[crm_prd_info]

--checking for NULL and Duplicate value for primary key(prd_id)
--No result should be returned
SELECT 
	p.prd_id,
	count(*)
FROM [Silver].[crm_prd_info] p
group by p.prd_id
having count(*) > 1 or p.prd_id is NULL

--Checking for Unwanted Spaces
--No result should be returned
SELECT
	p.prd_nm
FROM [Silver].[crm_prd_info] p
where p.prd_nm != TRIM(p.prd_nm)

--Checking for NULL or negative number
--No result should be returned
SELECT
	p.prd_cost
FROM [Silver].[crm_prd_info] p
where p.prd_cost < 0 or p.prd_cost IS NULL

--Data Standardization & Consistency
SELECT DISTINCT p.prd_line
FROM [Silver].[crm_prd_info] p

--Checking for Invalid Date Orders
--No result should be returned
SELECT
	*
FROM [Silver].[crm_prd_info] p
where p.prd_end_dt < p.prd_start_dt

--------------------------------------------------------------------------
--Checking the data quality in [Silver].[crm_sales_details]
-------------------------------------------------------------------------

--Check if dwh_created_date is added
SELECT * FROM [Silver].[crm_sales_details];

--Checking for Unwanted Spaces
--Result No data cleaning not required
--No data should be pulled
SELECT
	s.sls_ord_num
FROM [Silver].[crm_sales_details] s
where s.sls_ord_num != TRIM(s.sls_ord_num);

--Checking integrity
--For prd_key(there should be no key that is in sales table and not in product table)
--No data should be pulled
SELECT
	s.sls_prd_key
FROM [Silver].[crm_sales_details] s
where s.sls_prd_key NOT IN (SELECT prd_key FROM Silver.crm_prd_info);

--For cust_id(there should be no key that is in sales table and not in customer table)
--No data should be pulled
SELECT
	s.sls_cust_id
FROM [Silver].[crm_sales_details] s
where s.sls_cust_id NOT IN (SELECT cst_id FROM Silver.crm_cust_info);


--Checking for Invalid date order
--No data should be pulled
SELECT
	*
FROM [Silver].[crm_sales_details] s
where s.sls_order_dt > s.sls_ship_dt or s.sls_order_dt > s.sls_due_dt

--Check data consistency: Between Sales,Quantity and Price
-- Sales = Quantity * Price
-- Values must not be negative,NULL or zero
--No data should be pulled

SELECT
	s.sls_sales,
	s.sls_quantity,
	s.sls_price
FROM [Silver].[crm_sales_details] s
WHERE s.sls_sales != s.sls_quantity * s.sls_price
OR s.sls_sales IS NULL OR s.sls_quantity IS NULL OR s.sls_price IS NULL
OR s.sls_sales <= 0 OR s.sls_quantity <=0 OR s.sls_price <= 0
order by s.sls_sales , s.sls_quantity , s.sls_price;

--------------------------------------------------------------------------------------------
--Checking the data quality in [Silver].[erp_cust_az12]
--------------------------------------------------------------------------------------------

--Check if dwh_created_date is added
Select *
from [Silver].[erp_cust_az12];

--checking the validity  of bday
--Only future bday will be set to NULL
SELECT
	ec.bdate
FROM [Silver].[erp_cust_az12] ec
where ec.bdate < '1924-01-01' OR bdate > GETDATE();

--Data Standardization & Consistency
--For Low cardinality data
SELECT DISTINCT ec.gen
FROM [Silver].[erp_cust_az12] ec

--------------------------------------------------------------------------------------------
--Checking the data quality in [Silver].[erp_loc_a101]
--------------------------------------------------------------------------------------------

--Check if dwh_created_date is added
Select *
from [Silver].[erp_loc_a101];

--Data Standardization & Consistency
--For Low cardinality data
Select
	distinct be.cntry
from [Silver].[erp_loc_a101] be;
