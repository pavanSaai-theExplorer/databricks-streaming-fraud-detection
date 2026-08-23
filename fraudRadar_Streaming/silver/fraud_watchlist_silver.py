from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

"""
Silver Layer: Fraud Watchlist

Cleans and standardizes fraud watchlist data from the bronze layer.
Transformations include:
- Standardizing text fields to uppercase for consistent matching
- Converting timestamp formats to standard format
- Tracking data lineage through ingestion timestamps
"""

@dp.table(
    name="fraudradar.silver.fraud_watchlist",
    comment="Cleaned data for fraud watchlist"
)
def fraud_watchlist_silver() -> DataFrame:
    """
    Streaming table that cleanses fraud watchlist data from bronze layer.
    
    Returns:
        DataFrame: Cleaned watchlist data with standardized formats
    """
    # Read streaming data from bronze fraud watchlist table
    bronze_df = spark.readStream.table("fraudradar.bronze.fraud_watchlist")
    
    # Apply cleansing transformations
    cleaned_df = bronze_df.select(
                                # Standardize key identifiers to uppercase for consistent joins
                                F.upper(F.col("watchlist_id")).alias("watchlist_id"),
                                F.col("watch_type"),
                                F.upper(F.col("entity_id")).alias("entity_id"),
                                
                                # Standardize risk and action fields
                                F.upper(F.col("risk_level")).alias("risk_level"),
                                F.upper(F.col("action")).alias("action"),
                                
                                # Reason and status information
                                F.col("reason_code"),
                                F.col("reason_description"),
                                F.col("status"),
                                
                                # Convert timestamp from source format to standard timestamp
                                F.to_timestamp(F.col("effective_from"), "dd-MMM-yyyy HH:mm:ss").alias("effective_from"),
                                
                                # Reporter and location metadata
                                F.col("reported_by"),
                                F.col("reported_source"),
                                F.col("country"),
                                F.col("city"),
                                
                                # Data lineage tracking
                                F.col("source_file"),
                                F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
                                F.current_timestamp().alias("silver_ingestion_timestamp"))
    
    return cleaned_df
    
    




