
/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) AS cutomer_key,
	ci.cst_id					AS [customer_id],
	ci.cst_key					AS [customer_number],
	ci.cst_firstname			AS [first_name],
	ci.cst_lastname				AS [last_name],
	la.cntry					AS [country],
	ci.cst_marital_status		AS [marital_status],
	CASE 
		WHEN ci.cst_gndr != 'NA'
			THEN ci.cst_gndr
		ELSE COALESCE(ca.gen,'NA')
	END							AS [gender],
	ca.bdate					AS [birthdate],
	ci.cst_create_date			AS [create_date]
FROM [Silver].[crm_cust_info] ci
LEFT JOIN [Silver].[erp_cust_az12] ca
	ON  ci.cst_key = ca.cid
LEFT JOIN [Silver].[erp_loc_a101] la
	ON ci.cst_key = la.cid

GO
-- =============================================================================
-- Create Dimension: gold.dim_product
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.prd_key) AS [product_key],
	pn.prd_id				AS [product_id],
	pn.prd_key				AS [product_number],
	pn.prd_nm				AS [product_name],
	pn.cat_id				AS [category_id],	
	pc.cat					AS [category],
	pc.subcat				AS [subcategory],
	pc.maintenance			AS [maintenance],
	pn.prd_cost				AS [cost],
	pn.prd_line				AS [product_line],
	pn.prd_start_dt			AS [start_date]
FROM [Silver].[crm_prd_info] pn
LEFT JOIN [Silver].[erp_px_cat_g1v2] pc
	ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL --filtering out NULL data

GO
-- =============================================================================
-- Create Dimension: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
	cs.sls_ord_num				AS [order_number],
	dp.product_key				AS [product_key],
	dc.cutomer_key				AS [customer_key],
	cs.sls_order_dt				AS [order_date],
	cs.sls_ship_dt				AS [shipping_date],
	cs.sls_due_dt				AS [due_date],
	cs.sls_sales				AS [sales],
	cs.sls_quantity				AS [quantity],
	cs.sls_price				AS [price]
FROM [Silver].[crm_sales_details] cs
LEFT JOIN gold.dim_products dp
	ON cs.sls_prd_key = dp.product_number
LEFT JOIN gold.dim_customers dc
	ON cs.sls_cust_id = dc.customer_id;

GO