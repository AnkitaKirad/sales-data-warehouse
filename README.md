# 🧾 Sales Data Warehouse Project

This project is a foundational data warehousing solution built using SQL Server. It simulates integrating CRM and ERP data sources into a central data warehouse for analytical reporting. 
The main focus of this project is to **learn core data warehousing concepts**, including data modeling, architecture design, and data engineering fundamentals.

-------------------------------------------------------------------------------------------------

## 📌 Project Objective

To design and build a sales-focused data warehouse that demonstrates:
- End-to-end data integration from transactional systems (CRM and ERP)
- Use of **Medallion architecture** for modular data flow
- Implementation of **Star Schema**
- Handling **Slowly Changing Dimensions Type 1 (SCD1)**
- Mastery of **SQL for ETL**, data cleaning, and transformations

-------------------------------------------------------------------------------------------------

## 🧱 Data Architecture

This project follows the **Medallion Architecture** pattern:

- **Bronze Layer**: Raw CRM & ERP data (staging tables)
- **Silver Layer**: Cleaned and validated data
- **Gold Layer**: Star schema with fact and dimension tables for analysis

-------------------------------------------------------------------------------------------------

## 🗃️ Data Modeling Approach

Used the **Star Schema** for designing the analytical layer:

### 🟨 Dimension Tables
- `Dim_Customer`: Customer details from CRM & ERP
- `Dim_Product`: Product metadata from CRM & ERP

### 🟦 Fact Tables
- `Fact_Sales`: Sales transactions combining CRM and ERP data

-------------------------------------------------------------------------------------------------

## 🔧 Features & Techniques

| Feature                             | Description                                                                 |
|------------------------------------|-----------------------------------------------------------------------------|
| Data Cleaning                      | Removed duplicates, handled nulls, applied formatting and standardization  |
| SQL Window Functions               | Used to identify and resolve data quality issues                           |
| SCD Type 1                         | Handled dimension updates with overwrite logic                             |
| Surrogate Keys                     | Implemented for all dimension tables                                       |
| Analytical Queries                 | Created business insights using aggregated and filtered data               |
| 100% SQL-based                     | Project developed entirely in **SQL Server** (no external tools used)      |

-------------------------------------------------------------------------------------------------
## 📊 Sample Analytical Queries

-- Total Sales by Product Category
SELECT 
    p.Category, 
    SUM(f.sales) AS TotalSales
FROM [Gold].[fact_sales] f
JOIN [Gold].[dim_products] p 
	ON f.[product_key] = p.[product_key]
GROUP BY p.Category
ORDER BY TotalSales DESC;

-------------------------------------------------------------------------------------------------
## 🧠 Key Learnings

- Hands-on experience with data warehouse design and architecture
- Gained understanding of SCD Types, fact vs. dimension tables, and ETL best practices
- Improved SQL skills using window functions, joins, and aggregations
- Learned how to implement Medallion architecture in a traditional RDBMS

-------------------------------------------------------------------------------------------------
## 🛠️ Tech Stack

- Database: Microsoft SQL Server (MSSQL)
- Language: T-SQL
- Data Sources: Simulated CRM and ERP exports (CSV or manual mock data)

------------------------------------------------------------------------------------------------ 
## 📎 Note

This project uses mocked or simplified data, focused on learning key concepts over working with real-world complexity. It's designed for understanding data warehousing from the ground up.

------------------------------------------------------------------------------------------------
## 📣 Author

[ANKITA KIRAD]
[www.linkedin.com/in/ankitakirad]
[https://github.com/AnkitaKirad]
