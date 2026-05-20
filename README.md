# my-test-repo1

A test repository containing SQL and PySpark scripts for working with two tables — `products` and `transactions`.

## Contents

### sql_files/
- **code1.sql** — A basic SQL query that selects all records from `tbl1`.

### pyspark1_files/
- **create_tables.py** — Defines the schemas for the `products` and `transactions` tables using PySpark `StructType`.
- **insert_values.py** — Populates both tables with sample data (5 products, 7 transactions).
- **query_joined.py** — Performs an inner join between `transactions` and `products` on `product_id`, and computes `total_amount` (quantity × price).
