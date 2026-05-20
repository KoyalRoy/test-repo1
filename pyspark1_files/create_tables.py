from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType, DateType
)

spark = SparkSession.builder \
    .appName("CreateTables") \
    .getOrCreate()

# --- Products table schema ---
products_schema = StructType([
    StructField("product_id",   IntegerType(), nullable=False),
    StructField("product_name", StringType(),  nullable=False),
    StructField("category",     StringType(),  nullable=True),
    StructField("price",        DoubleType(),  nullable=False),
])

# --- Transactions table schema ---
transactions_schema = StructType([
    StructField("transaction_id",   IntegerType(), nullable=False),
    StructField("product_id",       IntegerType(), nullable=False),
    StructField("customer_id",      IntegerType(), nullable=False),
    StructField("quantity",         IntegerType(), nullable=False),
    StructField("transaction_date", DateType(),    nullable=False),
])

# Create empty DataFrames and register as temp views (acts as table definitions)
products_df = spark.createDataFrame([], products_schema)
products_df.createOrReplaceTempView("products")

transactions_df = spark.createDataFrame([], transactions_schema)
transactions_df.createOrReplaceTempView("transactions")

print("Tables created successfully.")
print("Products schema:")
products_df.printSchema()
print("Transactions schema:")
transactions_df.printSchema()

spark.stop()
