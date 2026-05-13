#  Azure Retail Customer Purchase Data Pipeline

> A production-grade, end-to-end Azure data engineering pipeline that aggregates multi-source retail data into a centralised analytics warehouse — built using the Medallion Architecture (Bronze → Silver → Gold).

![Azure](https://img.shields.io/badge/Microsoft_Azure-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)

---

##  Table of Contents

- [Project Overview](#-project-overview)
- [Architecture](#-architecture)
- [Azure Services Used](#-azure-services-used)
- [Project Structure](#-project-structure)
- [Data Sources](#-data-sources)
- [Pipeline Walkthrough](#-pipeline-walkthrough)
- [Business Insights Generated](#-business-insights-generated)
- [Dashboard](#-dashboard)
- [How to Reproduce](#-how-to-reproduce)
- [Key Learnings](#-key-learnings)
- [Technologies](#-technologies)

---

##  Project Overview

**Business Scenario:** A mid-to-large retail company operates both physical stores and an e-commerce platform. Sales data was scattered across three disconnected systems — store POS databases, online purchase APIs, and customer loyalty CSV files — making unified analysis impossible.

**Solution:** Built a fully automated Azure data pipeline that:
- Ingests data from 3 heterogeneous sources daily
- Cleans and transforms raw data using PySpark
- Loads analytics-ready data into a centralised Gold zone
- Powers interactive Power BI dashboards for business stakeholders

**Key Results:**
| Metric | Value |
|--------|-------|
| Total Revenue | $72,683.69 |
| Total Transactions | 2,000 |
| Average Order Value | $36.34 |
| Total Customers | 201 |
| Online Revenue | $42,471.86 (58%) |
| Store POS Revenue | $30,211.83 (42%) |
| Top Product | SKU-JUICE-003 ($781.55) |

---

##  Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │ Store POS DB │  │  Online API  │  │ Customer Demographics│  │
│  │ (CSV - 1000) │  │ (CSV - 1000) │  │    (CSV - 200)       │  │
│  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────┘  │
└─────────┼─────────────────┼─────────────────────┼─────────────┘
          │                 │                       │
          ▼                 ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│              AZURE DATA FACTORY (Orchestration)                  │
│         PL_Master_Retail → TR_Daily_Retail (2AM UTC)            │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│           BRONZE ZONE — ADLS Gen2 (Raw Data)                  │
│   bronze/store/transactions.csv                                  │
│   bronze/online/purchases.csv                                    │
│   bronze/customers/demographics.csv                              │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│          AZURE DATABRICKS (PySpark Transformations)              │
│   - Schema casting & type enforcement                            │
│   - Deduplication & null handling                                │
│   - Derived columns (total_amount = qty × price)                 │
│   - String normalisation (SKU, loyalty tier)                     │
│   - Dataset union (store + online transactions)                  │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│           GOLD ZONE — ADLS Gen2 (Analytics Ready)             │
│   gold/transactions/ (2000 rows — Parquet, partitioned)          │
│   gold/customers/    (200 rows  — Parquet)                       │
│                                                                  │
│   Databricks SQL Tables:                                         │
│   retail_gold.fact_transactions                                  │
│   retail_gold.dim_customer                                       │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              POWER BI (Analytics & Reporting)                    │
│   - Executive Summary Dashboard                                  │
│   - Revenue by Channel Analysis                                  │
│   - Customer Loyalty Tier Analysis                               │
│   - Top Products by Revenue                                      │
│   - Monthly Revenue Trend                                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## Azure Services Used

| Service | Purpose | Resource Name |
|---------|---------|---------------|
| Azure Resource Group | Project container | `rg-retail-pipeline` |
| ADLS Gen2 | Data lake storage (Bronze/Silver/Gold) | `retaildlsdaya2024` |
| Azure Data Factory V2 | Pipeline orchestration | `adf-retail-pipeline-daya` |
| Azure Databricks | PySpark transformations | `databricks-retail-daya` |
| Power BI | Business intelligence dashboards | `Retail Customer Purchase Analytics` |

---

## Project Structure

```
retail-pipeline-project/
│
├── data/
│   ├── bronze/
│   │   ├── store/
│   │   │   └── transactions.csv          # 1,000 store POS records
│   │   ├── online/
│   │   │   └── purchases.csv             # 1,000 online purchase records
│   │   └── customers/
│   │       └── demographics.csv          # 200 customer records
│   └── retail_dashboard.csv              # Combined dataset for Power BI
│
├── notebooks/
│   └── 01_Bronze_to_Silver_Transformation.ipynb  # PySpark notebook
│
├── adf-configs/
│   ├── linked-services/
│   │   └── LS_ADLS_Retail.json           # ADLS Gen2 linked service
│   ├── datasets/
│   │   ├── DS_StoreTransactions.json
│   │   ├── DS_OnlinePurchases.json
│   │   ├── DS_Customer.json
│   │   ├── DS_Sink_Store.json
│   │   ├── DS_Sink_Online.json
│   │   └── DS_Sink_Customer.json
│   └── pipelines/
│       ├── Copy_StoreTransactions.json
│       ├── Copy_OnlinePurchases.json
│       ├── Copy_Customer.json
│       └── PL_Master_Retail.json         # Master pipeline
│
├── sql-scripts/
│   └── create_gold_tables.sql            # Databricks SQL table creation
│
└── README.md
```

---

## Data Sources

### Source 1 — Store Transactions (POS System)
```
Columns: transaction_id, store_id, product_sku, quantity,
         unit_price, transaction_date, customer_id
Records: 1,000 rows
Format:  CSV
```

### Source 2 — Online Purchases (E-commerce API)
```
Columns: order_id, customer_id, product_sku, quantity,
         unit_price, order_date, channel
Records: 1,000 rows
Format:  CSV
```

### Source 3 — Customer Demographics (CRM System)
```
Columns: customer_id, name, email, city, state,
         loyalty_tier, join_date
Records: 200 rows
Format:  CSV
```

---

## Pipeline Walkthrough

### Step 1 — Data Ingestion (Azure Data Factory)

Three ADF Copy pipelines extract data from source files and land them in the Bronze zone:

```
Copy_StoreTransactions  → bronze/store/transactions.csv
Copy_OnlinePurchases    → bronze/online/purchases.csv
Copy_Customer           → bronze/customers/demographics.csv
```

All three are chained in `PL_Master_Retail` and triggered daily at 2AM UTC via `TR_Daily_Retail`.

### Step 2 — Data Transformation (Azure Databricks)

PySpark transformations applied in `01_Bronze_to_Silver_Transformation` notebook:

```python
# Key transformations applied:

# 1. Schema casting
df_store = df_store \
    .withColumn("quantity",   F.col("quantity").cast("integer")) \
    .withColumn("unit_price", F.col("unit_price").cast("double")) \
    .withColumn("transaction_date", F.to_date("transaction_date", "yyyy-MM-dd"))

# 2. Derived column
    .withColumn("total_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))

# 3. String normalisation
    .withColumn("product_sku", F.upper(F.trim(F.col("product_sku"))))

# 4. Deduplication
    .dropDuplicates(["transaction_id"])

# 5. Null removal
    .dropna(subset=["transaction_id", "customer_id"])

# 6. Dataset union (store + online)
df_all_transactions = df_store_clean.select(common_cols) \
    .unionByName(df_online_clean.select(common_cols))
```

### Step 3 — Gold Zone Loading

Clean data written to Gold zone as Parquet (columnar format — 10-100x faster than CSV for analytics):

```python
# Write to Gold zone as partitioned Parquet
df_all_transactions.write \
    .mode("overwrite") \
    .partitionBy("source_system") \
    .parquet("abfss://gold@retaildlsdaya2024.dfs.core.windows.net/transactions/")
```

### Step 4 — SQL Tables (Databricks SQL)

Delta tables registered in `retail_gold` database for SQL querying:

```sql
-- Create fact_transactions table
CREATE TABLE retail_gold.fact_transactions
USING DELTA
AS SELECT * FROM parquet.`abfss://gold@retaildlsdaya2024.dfs.core.windows.net/transactions/`

-- Create dim_customer table  
CREATE TABLE retail_gold.dim_customer
USING DELTA
AS SELECT * FROM parquet.`abfss://gold@retaildlsdaya2024.dfs.core.windows.net/customers/`
```

---

## Business Insights Generated

### Revenue by Channel
```sql
SELECT 
    source_system AS channel,
    COUNT(*) AS total_transactions,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value
FROM retail_gold.fact_transactions
GROUP BY source_system
ORDER BY total_revenue DESC
```

| Channel | Transactions | Total Revenue | Avg Order Value |
|---------|-------------|---------------|-----------------|
| Online | 1,000 | $42,471.86 | $42.47 |
| Store POS | 1,000 | $30,211.83 | $30.21 |

### Top 10 Products by Revenue
| Rank | Product SKU | Times Sold | Units | Revenue |
|------|------------|------------|-------|---------|
| 1 | SKU-JUICE-003 | 17 | 138 | $781.55 |
| 2 | SKU-BREAD-043 | 16 | 122 | $766.82 |
| 3 | SKU-JUICE-050 | 17 | 118 | $663.26 |
| 4 | SKU-JUICE-015 | 14 | 100 | $662.66 |
| 5 | SKU-APPLE-007 | 18 | 129 | $661.52 |

### Revenue by Customer Loyalty Tier
| Tier | Customers | Transactions | Total Revenue |
|------|-----------|-------------|---------------|
| SILVER | 74 | 716 | $25,455.84 |
| GOLD | 63 | 642 | $23,667.66 |
| BRONZE | 63 | 633 | $23,279.66 |

### Monthly Revenue Trend
| Period | Transactions | Revenue |
|--------|-------------|---------|
| April 2026 | 1,706 | $61,853.02 |
| May 2026 | 294 | $10,830.67 |

---

## 📊 Dashboard

The Power BI dashboard **Retail Customer Purchase Analytics** includes:

- 4 KPI Cards — Total Revenue, Total Transactions, Avg Order Value, Total Customers
- Revenue by Channel — Clustered bar chart (Online vs Store POS)
- Revenue by Loyalty Tier — Donut chart (Silver, Gold, Bronze)
- Monthly Revenue Trend — Line chart over time
- Top Products by Revenue — Horizontal bar chart



---

## How to Reproduce

### Prerequisites
- Azure subscription (free trial at azure.microsoft.com/free)
- Azure CLI installed
- Python 3.9+
- Power BI account

### Step 1 — Clone this repository
```bash
git clone https://github.com/DayaMeenakshiBalaSubbu/azure-retail-data-pipeline.git
cd azure-retail-pipeline
```

### Step 2 — Create Azure Resources
```bash
# Login to Azure
az login

# Create Resource Group
az group create \
  --name rg-retail-pipeline \
  --location eastus

# Create ADLS Gen2
az storage account create \
  --name retaildlsdaya2024 \
  --resource-group rg-retail-pipeline \
  --location eastus \
  --sku Standard_LRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true

# Create containers
az storage container create --name bronze --account-name retaildlsdaya2024
az storage container create --name silver --account-name retaildlsdaya2024
az storage container create --name gold   --account-name retaildlsdaya2024
```

### Step 3 — Upload Source Data
```bash
# Get storage key
STORAGE_KEY=$(az storage account keys list \
  --account-name retaildlsdaya2024 \
  --resource-group rg-retail-pipeline \
  --query "[0].value" --output tsv)

# Upload files to Bronze zone
az storage blob upload \
  --account-name retaildlsdaya2024 \
  --container-name bronze \
  --name store/transactions.csv \
  --file data/bronze/store/transactions.csv \
  --account-key $STORAGE_KEY
```

### Step 4 — Configure ADF
1. Create Azure Data Factory in Portal
2. Import linked service from `adf-configs/linked-services/`
3. Import datasets from `adf-configs/datasets/`
4. Import pipelines from `adf-configs/pipelines/`
5. Publish all and run `PL_Master_Retail`

### Step 5 — Run Databricks Notebook
1. Create Databricks workspace
2. Upload `notebooks/01_Bronze_to_Silver_Transformation.ipynb`
3. Configure storage connection with your account key
4. Run all cells

### Step 6 — Connect Power BI
1. Export Gold data as CSV from Databricks
2. Upload to Power BI
3. Build dashboard using the visuals described above

---

## Key Learnings

| Concept | What I Learned |
|---------|----------------|
| Medallion Architecture | Bronze (raw) → Silver (clean) → Gold (analytics-ready) |
| Azure Data Factory | Linked Services, Datasets, Copy Activities, Schedule Triggers |
| PySpark | DataFrame transformations, schema casting, deduplication, union |
| Parquet Format | Columnar storage — 10-100x faster than CSV for analytics |
| Data Partitioning | partitionBy() enables partition elimination for faster queries |
| Delta Tables | ACID transactions, time travel, schema enforcement |
| Power BI | KPI cards, bar charts, donut charts, line charts, themes |

---

## Technologies

| Category | Technology |
|----------|-----------|
| Cloud Platform | Microsoft Azure |
| Data Lake | Azure Data Lake Storage Gen2 |
| Orchestration | Azure Data Factory V2 |
| Transformation | Azure Databricks (Apache Spark 3.4, PySpark) |
| Storage Format | Parquet (columnar), CSV (source) |
| Query Engine | Databricks SQL |
| Visualisation | Microsoft Power BI |
| CLI | Azure CLI 2.86 |
| Languages | Python 3.11, SQL, PySpark |
| OS | macOS (development) |

---

## 👤 Author

**Daya Meenakshi Bala Subbu**

---

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

---

> Built as a capstone project to demonstrate end-to-end Azure data engineering skills covering ingestion, transformation, warehousing, and visualisation using the Medallion Architecture pattern.
