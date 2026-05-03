# FMCG Consolidated Pipeline Databricks

Databricks medallion pipeline project for an Fast-Moving Consumer Goods (FMCG) sales use case using `bronze`, `silver`, and `gold` layers with full-load and incremental-load processing for the main orders fact table.

## Repo Structure

```text
fmcg-consolidated-pipeline-databricks/
  consolidated_pipeline/
    1_setup/
    2_dimension_data_processing/
    3_fact_data_processing/
  sql/
    dashboard_analytics_queries.sql
```

## Pipeline Overview

- `1_setup`
  - Creates the `fmcg` catalog and `bronze`, `silver`, `gold` schemas
  - Provides utility variables
  - Builds a `dim_date` table

- `2_dimension_data_processing`
  - Processes customer, product, and pricing datasets
  - Loads raw source files into Bronze
  - Cleans and standardizes data in Silver
  - Publishes dimension-ready tables into Gold

- `3_fact_data_processing`
  - `1_full_load_fact.py` performs the initial full load for orders
  - `2_incremental_load_fact.py` handles daily incremental order files
  - Uses landing and processed folders in S3
  - Uses Delta Lake merge logic for fact-table updates

## Key Concepts Used

- Databricks widgets
- Unity Catalog
- Bronze / Silver / Gold architecture
- S3 landing and processed zones
- Delta Lake merge
- Full load and incremental load

## Main Catalog Objects

- `fmcg.bronze.*`
- `fmcg.silver.*`
- `fmcg.gold.sb_dim_*`
- `fmcg.gold.sb_fact_orders`
- `fmcg.gold.fact_orders`
- `fmcg.gold.dim_date`

## Dashboard Layer

The `sql/dashboard_analytics_queries.sql` file contains sample analytics queries for building a dashboard from the Gold layer.

