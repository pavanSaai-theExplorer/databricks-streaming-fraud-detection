from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import *
import json

"""
Silver Layer: Transactions

Parses and cleanses transaction data from Kafka streams ingested in the bronze layer.
Transformations include:
- JSON parsing from Kafka value payload
- Schema enforcement with explicit type definitions
- Flattening nested JSON structure
- Preserving Kafka metadata for lineage (topic, partition, offset, timestamp)
- Data quality validation with expectations
- Tracking ingestion timestamps across bronze and silver layers
"""

# Data quality expectations that drop invalid records
@dp.table(
    name="fraudradar.silver.transactions",
    comment="Parsed and cleaned data of transactions"
)
# Critical fields - drop records with NULL values
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_card_number", "card_number IS NOT NULL")
# Business rule validation - warn on invalid amounts but keep record
@dp.expect("valid_amount", "amount > 0")
def transactions_silver() -> DataFrame:
    """
    Streaming table that parses and cleanses transaction data from bronze layer.
    
    Reads JSON-encoded transaction events from Kafka (via bronze layer),
    enforces schema, and validates critical business fields.
    
    Returns:
        DataFrame: Cleaned and structured transaction data with Kafka metadata
    """
    # Read streaming data from bronze transactions table (Kafka source)
    bronze_df = spark.readStream.table("fraudradar.bronze.transactions")

    # Define explicit schema for transaction JSON payload
    # Ensures type safety and consistent parsing
    schema = StructType([
        # Transaction identifiers
        StructField("transaction_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("card_number", StringType()),
        
        # Merchant information
        StructField("merchant_id", StringType()),
        StructField("merchant_name", StringType()),
        StructField("merchant_category", StringType()),
        
        # Transaction details
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("transaction_type", StringType()),
        StructField("payment_channel", StringType()),
        StructField("device_id", StringType()),
        
        # Location information
        StructField("city", StringType()),
        StructField("country", StringType()),
        
        # Transaction metadata
        StructField("transaction_timestamp", TimestampType()),
        StructField("is_international", BooleanType()),
        StructField("status", StringType())
    ])

    # Parse JSON and preserve Kafka metadata for lineage tracking
    transformed_df = bronze_df.select(
        # Parse JSON value field using defined schema
        F.from_json(col("value"), schema).alias("data"),
        
        # Preserve Kafka metadata for lineage and debugging
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        
        # Carry forward bronze ingestion timestamp
        F.col("ingesttime").alias("bronze_ingestion_ts")
    ).select(
        # Flatten the nested JSON structure
        F.col("data.*"),
        
        # Keep all metadata columns
        F.col("kafka_topic"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_timestamp"),
        F.col("bronze_ingestion_ts"),
        
        # Add silver layer ingestion timestamp for data lineage
        F.current_timestamp().alias("silver_ingestion_ts")
    )

    return transformed_df
        














