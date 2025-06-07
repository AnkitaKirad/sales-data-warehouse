--------------------------------------------------------------------------
--Finding the data issues in [Bronze].[crm_cust_info]
-------------------------------------------------------------------------

SELECT
	*
FROM [Bronze].[crm_cust_info];

--checking for NULL and Duplicate value for primary key(cst_id)
SELECT 
	c.cst_id,
	count(*)
FROM [Bronze].[crm_cust_info] c
group by c.cst_id
having count(*) > 1 or c.cst_id is NULL

--Checking for Unwanted Spaces
SELECT
	c.cst_lastname
FROM [Bronze].[crm_cust_info] c
where c.cst_lastname != TRIM(c.cst_lastname)
--17 records

SELECT
	c.cst_firstname
FROM [Bronze].[crm_cust_info] c
where c.cst_firstname != TRIM(c.cst_firstname)
--15 records

SELECT
	c.cst_gndr
FROM [Bronze].[crm_cust_info] c
where c.cst_gndr != TRIM(c.cst_gndr)
--0 records

SELECT
	c.cst_marital_status
FROM [Bronze].[crm_cust_info] c
where c.cst_marital_status != TRIM(c.cst_marital_status)
--0 records

--Data Standardization & Consistency
--For Low cardinality data
SELECT DISTINCT c.cst_gndr
FROM [Bronze].[crm_cust_info] c

SELECT DISTINCT c.cst_marital_status
FROM [Bronze].[crm_cust_info] c

--------------------------------------------------------------------------
--Finding the data issues in [Bronze].[crm_cust_info]
-------------------------------------------------------------------------

SELECT * FROM [Bronze].[crm_prd_info]

--checking for NULL and Duplicate value for primary key(prd_id)
--Result No duplicates cleaning not reuired
SELECT 
	p.prd_id,
	count(*)
FROM [Bronze].[crm_prd_info] p
group by p.prd_id
having count(*) > 1 or p.prd_id is NULL

--Checking for Unwanted Spaces
--Result No data cleaning not required
SELECT
	p.prd_nm
FROM [Bronze].[crm_prd_info] p
where p.prd_nm != TRIM(p.prd_nm)

--Checking for NULL or negative number
--Result having null needs to be handled
SELECT
	p.prd_cost
FROM [Bronze].[crm_prd_info] p
where p.prd_cost < 0 or p.prd_cost IS NULL

--Data Standardization & Consistency
--For Low cardinality data
SELECT DISTINCT p.prd_line
FROM [Bronze].[crm_prd_info] p

--Checking for Invalid Date Orders
SELECT
	*
FROM [Bronze].[crm_prd_info] p
where p.prd_end_dt < p.prd_start_dt

--------------------------------------------------------------------------
--Finding the data issues in [Bronze].[crm_cust_info]
-------------------------------------------------------------------------

SELECT * FROM [Bronze].[crm_sales_details];

--Checking for Unwanted Spaces
--Result No data cleaning not required
SELECT
	s.sls_ord_num
FROM [Bronze].[crm_sales_details] s
where s.sls_ord_num != TRIM(s.sls_ord_num);

--Checking integrity
--For prd_key(there should be no key that is in sales table and not in product table)
SELECT
	s.sls_prd_key
FROM [Bronze].[crm_sales_details] s
where s.sls_prd_key NOT IN (SELECT prd_key FROM Silver.crm_prd_info);

--For cust_id(there should be no key that is in sales table and not in customer table)
SELECT
	s.sls_cust_id
FROM [Bronze].[crm_sales_details] s
where s.sls_cust_id NOT IN (SELECT cst_id FROM Silver.crm_cust_info);

--Checking for Invalid dates
--we have zero's  that should not be valid,
--Also the datatype is int it needs to be date
Select 
	s.sls_order_dt
FROM [Bronze].[crm_sales_details] s
where s.sls_order_dt <= 0
or len(s.sls_order_dt) != 8
or sls_order_dt > 20500101
or sls_order_dt < 19000101

Select 
	s.sls_ship_dt
FROM [Bronze].[crm_sales_details] s
where s.sls_ship_dt <= 0
or len(s.sls_ship_dt) != 8
or sls_ship_dt > 20500101
or sls_ship_dt < 19000101

Select 
	s.sls_order_dt
FROM [Bronze].[crm_sales_details] s
where s.sls_order_dt <= 0
or len(s.sls_order_dt) != 8
or sls_order_dt > 20500101
or sls_order_dt < 19000101

Select 
	s.sls_due_dt
FROM [Bronze].[crm_sales_details] s
where s.sls_due_dt <= 0
or len(s.sls_due_dt) != 8
or sls_due_dt > 20500101
or sls_due_dt < 19000101

--Checking for Invalid date order
SELECT
	*
FROM [Bronze].[crm_sales_details] s
where s.sls_order_dt > s.sls_ship_dt or s.sls_order_dt > s.sls_due_dt

--Check data consistency: Between Sales,Quantity and Price
-- Sales = Quantity * Price
-- Values must not be negative,NULL or zero

SELECT
	s.sls_sales,
	s.sls_quantity,
	s.sls_price
FROM [Bronze].[crm_sales_details] s
WHERE s.sls_sales != s.sls_quantity * s.sls_price
OR s.sls_sales IS NULL OR s.sls_quantity IS NULL OR s.sls_price IS NULL
OR s.sls_sales <= 0 OR s.sls_quantity <=0 OR s.sls_price <= 0
order by s.sls_sales , s.sls_quantity , s.sls_price

--------------------------------------------------------------------------
--Finding the data issues in [Bronze].[erp_cust_az12]
-------------------------------------------------------------------------

Select *
from [Bronze].[erp_cust_az12];

--checking the validity  of bday
SELECT
	ec.bdate
FROM [Bronze].[erp_cust_az12] ec
where ec.bdate < '1924-01-01' OR bdate > GETDATE();

--Data Standardization & Consistency
--For Low cardinality data
SELECT DISTINCT ec.gen
FROM [Bronze].[erp_cust_az12] ec

--------------------------------------------------------------------------
--Finding the data issues in [Bronze].[erp_loc_a101]
-------------------------------------------------------------------------

--Data Standardization & Consistency
--For Low cardinality data
Select
	distinct be.cntry
from [Bronze].[erp_loc_a101] be;

--------------------------------------------------------------------------
--Finding the data issues in [Bronze].[erp_px_cat_g1v2]
-------------------------------------------------------------------------

--Checking for unwanted spaces
Select * 
from [Bronze].[erp_px_cat_g1v2]
where cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);