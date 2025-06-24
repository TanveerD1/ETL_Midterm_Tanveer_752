# DSA2040_Midterm2025
# Tanveer 752
# ETL Pipeline Project - Sales Order Data Processing

## 1. Project Overview
This ETL pipeline processes sales order data from CSV files through comprehensive extraction, transformation, and loading phases. The project handles real-world data quality issues including missing values, duplicates, and inconsistent formatting to create a clean, analysis-ready dataset.

**Data Sources:**
- Raw sales data: 100 orders (Jan-Apr 2024)
- Incremental data: 10 new orders (May 2024)
- 7 fields: order_id, customer_name, product, quantity, unit_price, order_date, region

## 2. ETL Phases

### Extract Phase (etl_extract.ipynb)
- **Data Loading**: Successfully loads raw_data.csv (100 records) and incremental_data.csv (10 records)
- **Quality Assessment**: Identifies 18 missing quantities, 25 missing unit prices, 14 missing regions
- **Issue Detection**: Finds 1 exact duplicate record and 1 missing customer name
- **Data Profiling**: Analyzes data types, ranges, and distribution patterns

### Transform Phase (etl_transform.ipynb)
**5 Major Transformations Applied:**

1. **Data Cleaning**
   - Imputed missing quantities using product-based medians
   - Filled missing unit prices with product averages
   - Assigned regions based on customer history
   - Removed exact duplicate records

2. **Data Enrichment**
   - Calculated total_price (quantity × unit_price)
   - Added order_value_category (Low/Medium/High)
   - Created customer_order_frequency metrics
   - Generated quarterly order periods

3. **Structural Changes**
   - Converted order_date to proper DateTime format
   - Standardized text fields (proper case, trimmed spaces)
   - Rounded monetary values to 2 decimal places
   - Added data_quality_score (0-100 scale)

4. **Filtering & Validation**
   - Removed records with impossible calculations
   - Filtered out negative quantities or prices
   - Added validation flags for data integrity

5. **Categorization**
   - Created customer_tier (New/Regular/Loyal/VIP)
   - Mapped regions to standardized regional groups
   - Added seasonal classifications



### Load Phase (etl_load.ipynb)
- **Database Creation**: Stores clean data in SQLite databases
- **Verification Queries**: Confirms data integrity with SQL tests


## 3. Tools Used
- **Python 3.8+**: Core programming language
- **Pandas 1.5+**: Data manipulation and transformation
- **SQLite3**: Lightweight database for data storage
- **SQLAlchemy**: Database connection and ORM functionality
- **Jupyter Notebook**: Interactive development environment
- **Git**: Version control and project management

## 4. How to Run the Project

### Prerequisites
```bash
pip install pandas sqlite3 sqlalchemy jupyter matplotlib seaborn