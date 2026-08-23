from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

"""
Gold Layer: Transaction Count by Minute (Tumbling Window)

Computes real-time transaction counts using tumbling windows for fraud monitoring.
Implements stateful streaming aggregation with:
- Tumbling windows: 1-minute non-overlapping windows
- Watermarking: 10-minute tolerance for late-arriving events
- Use case: Track transaction volume at regular 1-minute intervals

Tumbling windows are non-overlapping and contiguous:
- Window 1: 10:00:00 - 10:01:00
- Window 2: 10:01:00 - 10:02:00
- Window 3: 10:02:00 - 10:03:00

Compare to sliding windows (see transactions_count_by_min_sliding_window.py)
which overlap for smoother metrics.
"""

@dp.table(
    name="fraudradar.gold.transactions_count_by_min",
    comment="Gets the count of transactions by minute"
)
def transactions_count_by_min_gold() -> DataFrame:
    """
    Streaming table that computes transaction counts using tumbling windows.
    
    Window Configuration:
    - Window size: 1 minute (duration of each window)
    - Window type: Tumbling (non-overlapping, contiguous windows)
    - Watermark: 10 minutes (maximum delay tolerated for late events)
    
    Returns:
        DataFrame: Window start/end times with transaction counts per window
    """
    # Read streaming transaction data from silver layer
    transactions = spark.readStream.table("fraudradar.silver.transactions")

    # Apply watermark to handle late-arriving data
    # Events arriving >10 minutes late will be dropped
    # Watermarking enables state cleanup and prevents unbounded state growth
    transactions_with_watermark = transactions.withWatermark("transaction_timestamp", "10 minutes")

    # Create tumbling windows and count transactions
    # Single parameter ("1 minute") creates non-overlapping windows
    # Each transaction belongs to exactly one window
    transactions_count_df = transactions_with_watermark.groupBy(F.window("transaction_timestamp", "1 minute")).count()

    # Extract window boundaries and count for clean output schema
    final_df = transactions_count_df.select(
        F.col("window.start").alias("window_start"),    # Start of the time window
        F.col("window.end").alias("window_end"),        # End of the time window
        F.col("count").alias("transactions_count")      # Total transactions in window
    )
    
    return final_df

