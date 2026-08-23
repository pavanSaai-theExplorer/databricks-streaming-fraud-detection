from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

"""
Gold Layer: Transaction Count by Minute (Sliding Window)

Computes real-time transaction counts using sliding windows for fraud monitoring.
Implements stateful streaming aggregation with:
- Sliding windows: 5-minute window size with 1-minute slide interval
- Watermarking: 10-minute tolerance for late-arriving events
- Use case: Detect sudden spikes in transaction volume that may indicate fraud

Sliding windows overlap, providing smooth continuous metrics. For example:
- Window 1: 10:00-10:05
- Window 2: 10:01-10:06 (overlaps with Window 1)
- Window 3: 10:02-10:07 (overlaps with Windows 1 & 2)
"""

@dp.table(
    name="fraudradar.gold.transactions_count_by_min_sliding_window",
    comment="Gets the count of transactions by minute"
)
def transactions_count_by_min_sliding_window_gold() -> DataFrame:
    """
    Streaming table that computes transaction counts using sliding windows.
    
    Window Configuration:
    - Window size: 5 minutes (duration of each window)
    - Slide interval: 1 minute (new window starts every minute)
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

    # Create sliding windows and count transactions
    # 5-minute windows sliding every 1 minute creates overlapping windows
    # Example: [10:00-10:05], [10:01-10:06], [10:02-10:07], ...
    transactions_count_df = transactions_with_watermark.groupBy(F.window("transaction_timestamp", "5 minute", "1 minute")).count()

    # Extract window boundaries and count for clean output schema
    final_df = transactions_count_df.select(
        F.col("window.start").alias("window_start"),    # Start of the time window
        F.col("window.end").alias("window_end"),        # End of the time window
        F.col("count").alias("transactions_count")      # Total transactions in window
    )
    
    return final_df

