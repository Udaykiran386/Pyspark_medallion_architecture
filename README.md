# Pyspark Medallion Architecture

An end-to-end **e-commerce data pipeline** built on Databricks using **PySpark** and the **Medallion Architecture** pattern (Bronze → Silver → Gold). The project ingests raw CSV data from Unity Catalog volumes, cleans and transforms it through progressive layers, and produces BI-ready dimension and fact tables in Delta format.

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         Unity Catalog: ecommerce                         │
├────────────────────┬─────────────────────┬──────────────────────────────┤
│      Bronze        │       Silver        │            Gold              │
│  (Raw Ingestion)   │  (Clean & Transform)│     (BI-Ready Tables)       │
├────────────────────┼─────────────────────┼──────────────────────────────┤
│ brz_brands         │ slv_brands          │ gld_dim_products             │
│ brz_category       │ slv_category        │ gld_dim_customers            │
│ brz_products       │ slv_products        │ gld_dim_date                 │
│ brz_customers      │ slv_customers       │ gld_fact_order_items         │
│ brz_calendar       │ slv_calendar        │                              │
│ brz_order_items    │ slv_order_items     │                              │
└────────────────────┴─────────────────────┴──────────────────────────────┘
        ▲                     ▲                     ▲
        │                     │                     │
  CSV files in          Data cleaning,        Joins, enrichment,
  UC Volumes            deduplication,         currency conversion,
  (append-only)         type casting           calculated measures
```

## Project Structure

```
Pyspark_medallion_architecture/
├── README.md
├── LICENSE
├── project_setup/
│   └── project_setup              # Creates the ecommerce catalog and bronze/silver/gold schemas
├── medallion_architecture_project/
│   ├── medallion_processing_dim/   # Dimension tables pipeline
│   │   ├── dim_bronze              # Raw ingestion of brands, category, products, customers, date
│   │   ├── dim_silver             # Cleaning, deduplication, anomaly fixes, type casting
│   │   └── dim_gold               # BI-ready dimension tables with joins and enrichment
│   └── medallion_architecture_facts/  # Fact tables pipeline
│       ├── 1_fact_bronze           # Raw ingestion of order items
│       ├── 2_fact_silver           # Cleaning, type casting, deduplication
│       ├── 3_fact_gold             # BI-ready fact table with measures and currency conversion
│       └── Fact_pipeline.py        # Consolidated pipeline (Bronze→Silver→Gold in one script)
```

## Data Sources

All source data resides in Unity Catalog volumes under `/Volumes/ecommerce/source_data/raw/`:

| Source | Path | Records | Key Columns |
| --- | --- | --- | --- |
| Brands | `brands/*.csv` | ~10 | brand_code, brand_name, category_code |
| Category | `category/*.csv` | ~10 | category_code, category_name |
| Products | `products/*.csv` | 50,000 | product_id, sku, category_code, brand_code, color, size, material, weight_grams, length_cm, width_cm, height_cm, rating_count |
| Customers | `customers/*.csv` | 300,000 | customer_id, phone, country_code, country, state |
| Date | `date/*.csv` | 95 | date, year, day_name, quarter, week_of_year |
| Order Items | `order_items/landing/*.csv` | ~187,000 | dt, order_ts, customer_id, order_id, item_seq, product_id, quantity, unit_price_currency, unit_price, discount_pct, tax_amount, channel, coupon_code |

## Dimension Pipeline

### Bronze Layer (`dim_bronze`)

Reads raw CSV files with explicit schema definitions and adds metadata columns (`_source_file`, `ingested_at`). Writes to Delta tables in overwrite mode.

**Tables produced:**
- `ecommerce.bronze.brz_brands`
- `ecommerce.bronze.brz_category`
- `ecommerce.bronze.brz_products`
- `ecommerce.bronze.brz_customers`
- `ecommerce.bronze.brz_calendar`

### Silver Layer (`dim_silver`)

Reads Bronze tables and applies data-quality fixes:

| Entity | Cleaning Operations |
| --- | --- |
| **Brands** | Trim whitespace, strip non-alphanumeric characters from `brand_code`, fix category code anomalies (GROCERY→GRCY, BOOKS→BKS, TOYS→TOY) |
| **Category** | Remove duplicate `category_code` rows, standardize to uppercase |
| **Products** | Strip `g` suffix from `weight_grams` → int, replace comma with dot in `length_cm` → float, uppercase `category_code` and `brand_code`, fix spelling mistakes (Coton→Cotton, Alumium→Aluminium, Ruber→Rubber), convert negative `rating_count` to absolute values |
| **Customers** | Drop 300 rows with null `customer_id`, fill 29,964 null phone values with "Not Available" |
| **Calendar** | Convert string date (`dd-MM-yyyy`) to date type, remove 3 duplicate rows, capitalize `day_name`, convert negative `week_of_year` to positive, enrich `quarter` to `Q3-2025` format and `week` to `Week31-2025` format |

**Tables produced:**
- `ecommerce.silver.slv_brands`
- `ecommerce.silver.slv_category`
- `ecommerce.silver.slv_products`
- `ecommerce.silver.slv_customers`
- `ecommerce.silver.slv_calendar`

### Gold Layer (`dim_gold`)

Builds BI-ready dimension tables by joining Silver tables and enriching with derived columns:

| Table | Description |
| --- | --- |
| `ecommerce.gold.gld_dim_products` | Joins products + brands + category on `category_code` to enrich product dimension with brand name and category name |
| `ecommerce.gold.gld_dim_customers` | Joins customers with a region mapping (India, Australia, UK, US states → regions), fills unmapped regions with "Other" |
| `ecommerce.gold.gld_dim_date` | Adds `date_id` (yyyyMMdd), `month_name`, and `is_weekend` flag; reorders columns for BI consumption |

## Fact Pipeline

### Bronze Layer (`1_fact_bronze`)

Ingests order items CSV files from the landing zone with an explicit schema (all strings to accommodate data anomalies). Adds `file_name` and `ingest_timestamp` metadata. Writes to `ecommerce.bronze.brz_order_items` in overwrite mode.

### Silver Layer (`2_fact_silver`)

Reads Bronze order items and applies transformations:
- Drop duplicates on `(order_id, item_seq)`
- Convert word quantities ("Two" → 2) and cast to int
- Strip `$` from `unit_price` → double
- Strip `%` from `discount_pct` → double
- Parse `dt` as date, `order_ts` as timestamp (with fallback format)
- Standardize channel names (web → Website, app → Mobile)
- Standardize `coupon_code` (nullify empty/null strings, lowercase)

Writes to `ecommerce.silver.slv_order_items`.

### Gold Layer (`3_fact_gold`)

Calculates financial measures and builds the final fact table:
- `gross_amount` = quantity × unit_price
- `discount_amount` = ceil(gross_amount × discount_pct / 100)
- `sale_amount` = gross_amount − discount_amount + tax_amount
- `date_id` = yyyyMMdd
- `coupon_flag` = 1 if coupon_code is not null, else 0
- **Currency conversion** to INR using fixed FX rates (INR: 1.0, AED: 24.18, AUD: 57.55, CAD: 62.93, GBP: 117.98, SGD: 68.18, USD: 88.29)
- `sale_amount_inr` = ceil(sale_amount × inr_rate)

Writes to `ecommerce.gold.gld_fact_order_items`.

### Consolidated Pipeline (`Fact_pipeline.py`)

A single Python script that runs the entire Bronze → Silver → Gold flow for order items with advanced features:
- **Append-only Bronze** with file versioning — only new or re-uploaded files are ingested; old data is never touched
- **Schema evolution** — new columns are added automatically; missing columns become NULL
- **Latest-version selection** — Silver and Gold are always rebuilt from the latest version of each business date
- **Spark + Pandas fallback** — if Spark cannot read a file (bad encoding, odd quotes), Pandas is used as a fallback reader
- **One-time migration** — automatically adds versioning columns to legacy Bronze tables and back-fills existing rows

## Setup Instructions

1. **Run `project_setup`** — creates the `ecommerce` catalog and `bronze`, `silver`, `gold` schemas
2. **Upload source data** — place CSV files in the corresponding Unity Catalog volume paths under `/Volumes/ecommerce/source_data/raw/`
3. **Run dimension pipeline** — execute notebooks in order: `dim_bronze` → `dim_silver` → `dim_gold`
4. **Run fact pipeline** — execute notebooks in order: `1_fact_bronze` → `2_fact_silver` → `3_fact_gold`
   - Alternatively, run `Fact_pipeline.py` for a consolidated single-script execution

## Technologies Used

- **Databricks** (Serverless compute, AWS)
- **PySpark** (DataFrame API, SQL functions)
- **Delta Lake** (ACID transactions, schema evolution)
- **Unity Catalog** (governance, volumes, three-layer namespace)
- **Medallion Architecture** (Bronze → Silver → Gold)

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.