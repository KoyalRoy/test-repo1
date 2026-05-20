from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType, DateType
)
from pyspark.sql.functions import col, round as spark_round
from datetime import date

spark = SparkSession.builder \
    .appName("JoinQuery") \
    .getOrCreate()

# --- Recreate products ---
products_schema = StructType([
    StructField("product_id",   IntegerType(), nullable=False),
    StructField("product_name", StringType(),  nullable=False),
    StructField("category",     StringType(),  nullable=True),
    StructField("price",        DoubleType(),  nullable=False),
])

products_data = [
    (1, "Laptop",     "Electronics", 999.99),
    (2, "Headphones", "Electronics", 149.99),
    (3, "Desk Chair", "Furniture",   249.99),
    (4, "Notebook",   "Stationery",   4.99),
    (5, "Monitor",    "Electronics", 399.99),
]

products_df = spark.createDataFrame(products_data, products_schema)

# --- Recreate transactions ---
transactions_schema = StructType([
    StructField("transaction_id",   IntegerType(), nullable=False),
    StructField("product_id",       IntegerType(), nullable=False),
    StructField("customer_id",      IntegerType(), nullable=False),
    StructField("quantity",         IntegerType(), nullable=False),
    StructField("transaction_date", DateType(),    nullable=False),
])

transactions_data = [
    (101, 1, 201, 1, date(2026, 1, 10)),
    (102, 2, 202, 2, date(2026, 1, 15)),
    (103, 3, 203, 1, date(2026, 2, 5)),
    (104, 4, 201, 5, date(2026, 2, 20)),
    (105, 5, 204, 1, date(2026, 3, 8)),
    (106, 1, 202, 2, date(2026, 3, 12)),
    (107, 2, 205, 1, date(2026, 4, 1)),
]

transactions_df = spark.createDataFrame(transactions_data, transactions_schema)

# --- Join transactions with products on product_id ---
joined_df = transactions_df.join(products_df, on="product_id", how="inner") \
    .select(
        col("transaction_id"),
        col("customer_id"),
        col("transaction_date"),
        col("product_name"),
        col("category"),
        col("quantity"),
        col("price"),
        spark_round(col("quantity") * col("price"), 2).alias("total_amount"),
    ) \
    .orderBy("transaction_date")

print("Transactions joined with Products:")
joined_df.show()

spark.stop()
