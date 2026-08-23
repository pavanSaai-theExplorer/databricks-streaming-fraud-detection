"""
Gold Layer - Fraud Card Alert Detection

Detects fraudulent transactions in real-time by matching card numbers against the fraud watchlist.
Uses stream-stream join pattern with watermarks for late data handling.

Data Flow:
    Silver Transactions (stream) + Silver Watchlist (stream) + Silver Customers (batch)
    -> Stream-Stream Join -> Stream-Batch Enrichment -> Fraud Alerts

Join Architecture:
    1. Stream-Stream Join: Transactions ⋈ Watchlist (on card_number = entity_id)
       - Both streams watermarked to handle late arrivals
       - Inner join: Only flagged cards generate alerts
       - Stateful operation: Maintains join state within watermark window
    
    2. Stream-Batch Join: Result ⋈ Customers (for contact info)
       - Left join: Alert generated even if customer record missing
       - No watermark needed for batch side

Watermark Strategy:
    - 10-minute watermark on both transaction_timestamp and effective_from
    - Allows late events up to 10 minutes past event time
    - Enables state cleanup after watermark passes
    - Critical for preventing unbounded state growth in long-running streams

Use Case:
    Real-time fraud prevention by blocking transactions from cards on the watchlist
    before they complete, minimizing financial loss.
"""

from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name="fraudradar.gold.fraud_card_alert",
    comment="Alert data from the Fraud Radar watchlist"
)
def fraud_card_alert_gold() -> DataFrame:
    """
    Streaming table that detects fraud by matching transactions against watchlist.
    
    Implements stream-stream join to catch fraud in real-time as both transactions
    and watchlist updates arrive. This is more powerful than stream-static join
    because it immediately reacts to new watchlist entries without waiting for
    batch updates.
    
    Returns:
        DataFrame: Comprehensive fraud alerts with:
            - Alert identifiers and metadata (alert_id, type, timestamp)
            - Complete transaction details (amount, merchant, location, etc.)
            - Watchlist match details (risk level, action, reason)
            - Customer contact information (for immediate notification)
    """
    
    # ========================================
    # Read Source Tables
    # ========================================
    # Streaming sources for real-time fraud detection
    # Both are streams to enable immediate reaction to:
    # - New transactions (potential fraud events)
    # - New watchlist entries (newly flagged cards)
    transactions = spark.readStream.table("fraudradar.silver.transactions")
    fraud_watch_list = spark.readStream.table("fraudradar.silver.fraud_watchlist")
    
    # Batch source for customer enrichment (dimension table)
    # Read as batch since customer data changes infrequently
    # and doesn't affect fraud detection logic (only notification)
    customers = spark.read.table("fraudradar.silver.customers")

    # ========================================
    # Apply Watermarks for Stream-Stream Join
    # ========================================
    # Watermarks are REQUIRED for stream-stream joins to:
    # 1. Define how long to wait for late data (10 minutes here)
    # 2. Enable state cleanup (drop old join state after watermark passes)
    # 3. Bound memory usage (prevents unbounded state growth)
    #
    # Events arriving >10 minutes late will be dropped from the join
    # Both streams must be watermarked on their respective event-time columns
    transactions_with_watermark = transactions.withWatermark("transaction_timestamp", "10 minutes")
    fraud_watchlist_with_watermark = fraud_watch_list.withWatermark("effective_from", "10 minutes")

    # ========================================
    # Join Transactions with Watchlist
    # ========================================
    # Step 1: Stream-stream inner join
    # - Matches transactions where card_number is on watchlist (entity_id)
    # - Inner join = only flagged cards produce alerts (non-flagged cards filtered out)
    # - Stateful operation: maintains buffered state for both streams
    # - Join state bounded by watermark (cleaned up after 10 min)
    #
    # Step 2: Stream-batch left join for customer enrichment
    # - Add customer contact info for notification
    # - Left join = alert still generated if customer record missing
    # - Not part of fraud detection logic (pure enrichment)
    df_joined = (transactions_with_watermark
                 .join(fraud_watchlist_with_watermark, 
                       transactions_with_watermark.card_number == fraud_watch_list.entity_id, how="inner")
                 .join(customers, transactions_with_watermark.customer_id == customers.customer_id, how="left"))

    # ========================================
    # Build Fraud Alert Records
    # ========================================
    # Create comprehensive alert records with all context needed for:
    # - Immediate blocking/review (risk level, action)
    # - Investigation (full transaction details)
    # - Customer notification (contact info)
    # - Audit trail (timestamps, sources, reasons)
    fraud_alert = df_joined.select(

            # ===== Alert Identification =====
            # Unique composite key from transaction + watchlist for idempotency
            F.concat_ws("-", F.lit("FRAUD"), F.col("transaction_id"), F.col("watchlist_id")).alias("alert_id"),
            F.lit("FRAUD_WATCHLIST_MATCH").alias("alert_type"),
            F.current_timestamp().alias("alert_timestamp"),  # When alert was generated (not transaction time)
            
            # ===== Transaction Details =====
            # Complete transaction context for fraud investigation
            transactions_with_watermark.transaction_id,
            transactions_with_watermark.customer_id,
            
            # Customer contact for immediate notification
            customers.email.alias("customer_email"),
            F.concat_ws(" ", customers.first_name, customers.last_name).alias("customer_name"),
            
            # Flagged card and transaction value
            transactions_with_watermark.card_number,      # The card that matched watchlist
            transactions_with_watermark.amount,           # Transaction value
            transactions_with_watermark.currency,
            
            # Merchant details (helps identify fraud patterns)
            transactions_with_watermark.merchant_id,
            transactions_with_watermark.merchant_name,
            transactions_with_watermark.merchant_category,
            
            # Transaction method and device (fraud indicators)
            transactions_with_watermark.transaction_type,
            transactions_with_watermark.payment_channel,
            transactions_with_watermark.device_id,
            
            # Location data (compare with watchlist location)
            transactions_with_watermark.city.alias("transaction_city"),
            transactions_with_watermark.country.alias("transaction_country"),
            
            # Timing and status
            transactions_with_watermark.transaction_timestamp,
            transactions_with_watermark.is_international,
            transactions_with_watermark.status.alias("transaction_status"),
            
            # ===== Fraud Watchlist Details =====
            # Why this card is flagged and what action to take
            F.col("watchlist_id"),                 # Watchlist entry identifier
            F.col("watch_type"),                   # Type of fraud watch
            F.col("risk_level"),                   # HIGH, MEDIUM, LOW - severity assessment
            F.col("action"),                       # BLOCK, REVIEW, MONITOR - recommended action
            F.col("reason_code"),                  # Standardized reason code
            F.col("reason_description"),           # Human-readable explanation
            F.col("effective_from").alias("watchlist_effective_from"),  # When card was flagged
            
            # Watchlist provenance (who reported this fraud)
            F.col("reported_by"),                  # Person/system that flagged the card
            F.col("reported_source"),              # Original fraud report source
            
            # Location where fraud was reported (compare with transaction location)
            fraud_watchlist_with_watermark.city.alias("watchlist_city"),
            fraud_watchlist_with_watermark.country.alias("watchlist_country")
    )
    
    return fraud_alert