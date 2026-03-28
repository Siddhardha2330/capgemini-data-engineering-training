# Phase 3 — SQL & PySpark (customers)

## Layout

| Path | Purpose |
|------|---------|
| `sql_queries.sql` | DDL, seed data, and guided SQL exercises |
| `pyspark_code.py` | Starter `DataFrame` and guided PySpark / Spark SQL exercises |
| `outputs/` | Save exports, CSVs, or screenshots from your runs here |

## Starter SQL tables

- Table `customers` with columns: `customer_id`, `customer_name`, `city`, `age`
- Three sample rows (Ravi, Sita, Arun)

## Starter PySpark

- Same data as a `DataFrame`, registered as temp view `customers`, with `show()`

## Guided exercises

1. Show all customers  
2. Show customers from Chennai  
3. Show customers with age > 25  
4. Show only `customer_name` and `city`  
5. Count customers city-wise  

Work in `sql_queries.sql` and `pyspark_code.py`; optional answer snippets are included as comments for self-check.
