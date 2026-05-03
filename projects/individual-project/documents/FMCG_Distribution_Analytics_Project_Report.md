# FMCG Distribution Analytics Platform

## 1. Project Overview

The FMCG Distribution Analytics Platform is a Databricks Lakehouse project designed to transform raw operational data into trusted business insights for sales, distribution, inventory, and management reporting.

The project follows a Medallion architecture:

- Bronze: raw ingested data
- Silver: cleaned, validated, and business-ready data
- Gold: aggregated KPI and reporting tables

The implementation uses:

- Databricks
- PySpark
- Delta Lake
- Delta Live Tables (DLT)
- Unity Catalog governance concepts

## 2. Business Domain

This project belongs to the FMCG domain, which focuses on high-volume, fast-moving products sold through distributors, retailers, and multiple trade channels.

Key business entities in this project:

- Distributor
- Retailer
- SKU
- Sales transaction
- Payment
- Inventory position

Important domain terms:

- FMCG: Fast Moving Consumer Goods
- SKU: Stock Keeping Unit
- GT: General Trade
- MT: Modern Trade

## 3. Business Problem

The business needs a platform that can:

- provide end-to-end visibility into distribution activity
- track sales at SKU, distributor, and retailer level
- identify stock risks such as stockout, overstock, and aging inventory
- improve reporting freshness
- support governed and auditable analytics

## 4. Data Sources

The main datasets used in the current pipeline are:

- customers
- orders
- order items
- payments
- products
- sellers

In the code, these are read from Bronze volume locations such as:

- `/Volumes/fmcg/bronze/customers/customers_raw.csv`
- `/Volumes/fmcg/bronze/orders/orders_raw.csv`
- `/Volumes/fmcg/bronze/order_items/order_items_raw.csv`
- `/Volumes/fmcg/bronze/payments/order_payments_raw.csv`
- `/Volumes/fmcg/bronze/products/products_raw.csv`
- `/Volumes/fmcg/bronze/seller/sellers_raw.csv`

## 5. Pipeline Architecture

### Bronze Layer

The Bronze layer ingests raw source data with minimal modification.

Bronze tables currently defined:

- customers_raw
- orders_raw
- order_items_raw
- payments_raw
- products_raw
- sellers_raw

Technical behavior:

- explicit schemas are defined for each source
- files are read as CSV
- lineage metadata is added

Metadata columns applied in Bronze:

- `_ingest_ts`
- `_source_file`
- `_source_system`
- `_batch_id`

### Silver Layer

The Silver layer converts raw transactional data into trusted business-ready data.

Silver tables currently defined:

- region_lookup
- quarantine_sales
- quarantine_monitoring
- silver_sales

Main Silver transformations:

- join raw orders, order items, and payments
- validate records
- isolate invalid records into quarantine
- standardize values such as payment type
- derive business fields such as quantity, net_amount, sales_value, region, and channel
- deduplicate transaction records
- produce final trusted sales fact table

### Gold Layer

The Gold layer creates reporting and KPI tables from Silver data.

Gold tables currently defined in the main pipeline:

- sales_summary
- sku_performance
- distributor_performance
- inventory_snapshot
- stock_aging

These tables are designed for dashboarding, reporting, and business decision-making.

## 6. Silver Layer Logic

### region_lookup

Creates a small mapping from state code to region.

Business use:
- region-based analysis

### quarantine_sales

Captures invalid records based on rules such as:

- price less than or equal to zero
- missing purchase date
- future purchase date

Business use:
- bad data is isolated before it affects KPIs

### quarantine_monitoring

Counts quarantined records by batch.

Business use:
- helps monitor pipeline quality

### silver_sales

This is the main conformed fact table.

Transformations:

- joins orders, order_items, and payments
- enriches using master-style dimensions
- standardizes payment type
- assumes quantity = 1 where source quantity is not available
- derives sales_value
- validates records
- removes duplicates
- maps distributor state to region
- derives trade channel

Business naming in Silver:

- customer_id -> retailer_id
- seller_id -> distributor_id
- product_id -> sku_id
- order_id -> invoice_id

## 7. Gold Layer Insights

### sales_summary

Insights:

- daily sales trend
- total revenue
- total quantity
- total orders
- performance by region and channel

### sku_performance

Insights:

- top-performing SKUs
- category contribution
- revenue share percent
- SKU ranking

### distributor_performance

Insights:

- distributor-wise sales contribution
- order count
- quantity handled
- approximate fill rate
- distributor ranking

### inventory_snapshot

Insights:

- estimated stock position
- stockout risk
- overstock risk

### stock_aging

Insights:

- average stock age
- quantity at risk
- stock aging buckets

## 8. Governance Implementation

Governance is applied in this project through layer separation, metadata, and controlled consumption.

Layer-wise accessibility model:

- Bronze: Data Engineers and Auditors
- Silver: Data Engineers and Analysts
- Gold: Analysts, Business Users, and Reporting Teams

Governance mechanisms present in the solution:

- clear Bronze, Silver, Gold separation
- lineage metadata columns in Bronze
- quarantine tables in Silver
- business-ready consumption in Gold
- Unity Catalog-oriented access design

## 9. Lineage and Auditability

Lineage is technically supported by:

- DLT dependency chain across Bronze -> Silver -> Gold
- source-tracking metadata columns
- reusable table names and transformation flow

Auditability is supported by:

- `_ingest_ts` to track load time
- `_source_file` to identify source file
- `_source_system` to identify origin system
- `_batch_id` to identify processing batch
- quarantine tables for rejected data

## 10. Performance and Optimization

Current performance-oriented practices observed:

- explicit schemas in Bronze
- `delta.autoOptimize.optimizeWrite = true`
- `delta.autoOptimize.autoCompact = true`
- pre-aggregated Gold KPI tables

Documented optimization strategy:

- OPTIMIZE on Silver fact table
- ZORDER by `sku_id` and `distributor_id`

## 11. Key Risks and Limitations

Current known limitations from the reviewed implementation:

- some KPIs are estimated rather than sourced from dedicated inventory datasets
- quantity is assumed as 1 in Silver
- channel derivation is simplified using state codes
- inventory snapshot is simulated from sales logic

These should be clearly stated during presentation to avoid over-claiming business accuracy.

## 12. Expected Business Value

The platform is designed to deliver:

- faster and more reliable reporting
- better SKU and distributor visibility
- improved stock monitoring
- reduced reporting risk through quarantine handling
- stronger governance and traceability

## 13. Conclusion

This project establishes a governed FMCG analytics foundation using Databricks Lakehouse principles. It organizes raw data into a trusted analytical flow, supports KPI creation, and prepares the platform for future enhancements such as stronger streaming, richer governance controls, and broader supply chain intelligence.
