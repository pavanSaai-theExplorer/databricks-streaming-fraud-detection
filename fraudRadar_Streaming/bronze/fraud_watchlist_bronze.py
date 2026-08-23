from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

"""
Bronze Layer: Fraud Watchlist from Cloud Files

Ingests fraud watchlist data from JSON files in Unity Catalog Volume using Auto Loader.
Implements:
- Auto Loader (cloudFiles) for incremental file processing
- Automatic schema inference with type detection
- Rescued data column for schema evolution and malformed records
- File metadata preservation for lineage and debugging

Auto Loader benefits:
- Automatically detects and processes only new files
- Handles schema evolution gracefully
- Scalable file discovery (no need to list all files)
- Exactly-once processing guarantees

Bronze layer philosophy:
- Preserve raw data as ingested from source
- Keep all fields including rescued data for unmatched columns
- Track source file and ingestion timestamp for lineage
"""

@dp.table(
    name="fraudradar.bronze.fraud_watchlist",
    comment="Raw stream data from Autoloader for fraud watchlist"
)
def fraud_watchlist_bronze() -> DataFrame:
    """
    Streaming table that ingests fraud watchlist JSON files using Auto Loader.
    
    Source: Unity Catalog Volume (/Volumes/fraudradar/source/fraud_watchlist/source_data/)
    Format: JSON files with automatic schema inference
    
    Auto Loader Configuration:
    - Format: JSON
    - Schema inference: Enabled with column type detection
    - Processing: Incremental (only new files since last run)
    - Rescued data: Captured for schema mismatches and malformed records
    
    Returns:
        DataFrame: Raw watchlist records with source file metadata
    """
    # ========================================
    # Read Streaming Data with Auto Loader
    # ========================================
    # Auto Loader (cloudFiles) automatically:
    # - Discovers new files in the volume path
    # - Maintains checkpoint to track processed files
    # - Processes each file exactly once
    # - Handles schema inference and evolution
    streaming_data = (spark.readStream.format("cloudFiles") 
             .option("cloudFiles.format", "json")                  # Source file format
             .option("cloudFiles.inferColumnTypes", "true")       # Infer types (not just strings)
             .load("/Volumes/fraudradar/source/fraud_watchlist/source_data/"))  # UC Volume path
    
    # ========================================
    # Select Watchlist Fields and Metadata
    # ========================================
    # Explicitly select expected columns to define output schema
    parsed_streaming_data = streaming_data.select(
                            # Watchlist identifiers
                            F.col("watchlist_id"),              # Unique ID for this watchlist entry
                            F.col("watch_type"),                # Type of watch (CARD, ACCOUNT, etc.)
                            F.col("entity_id"),                 # The entity being watched (card number, account ID)
                            
                            # Risk and action details
                            F.col("risk_level"),                # HIGH, MEDIUM, LOW
                            F.col("action"),                    # BLOCK, REVIEW, MONITOR
                            F.col("reason_code"),               # Standardized reason code
                            F.col("reason_description"),        # Human-readable reason
                            
                            # Status and timing
                            F.col("status"),                    # Active status of watchlist entry
                            F.col("effective_from"),            # When this entry became effective
                            
                            # Reporter information
                            F.col("reported_by"),               # Who reported this fraud
                            F.col("reported_source"),           # Source system of the report
                            
                            # Location information
                            F.col("country"),                   # Country where fraud reported
                            F.col("city"),                      # City where fraud reported
                            
                            # Schema evolution and error handling
                            F.col("_rescued_data"),             # Malformed/unexpected fields (NULL if none)
                            
                            # Source metadata for lineage
                            F.col("_metadata.file_path").alias("source_file"),     # Path to source file
                            F.current_timestamp().alias("ingestion_timestamp"))    # When ingested into bronze
    
    return parsed_streaming_data
    
    




