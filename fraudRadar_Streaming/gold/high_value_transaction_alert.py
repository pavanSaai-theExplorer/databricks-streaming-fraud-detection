from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

"""
Gold Layer: High Value Transaction Alerts

Generates real-time alerts for transactions that exceed customer-defined limits.
Implements stream-static join pattern:
- Streaming: Transaction events from silver layer
- Static: Customer profiles with transaction limits (dimension table)
- Alert trigger: When transaction amount > customer's transaction_limit

Use case: Customer protection by flagging unusual high-value transactions
that may indicate card theft, fraud, or unauthorized use.
"""

@dp.table(
    name="fraudradar.gold.high_value_transaction_alert",
    comment="Alert details of the transaction where it exceeding the value set by the customer"
)
def high_value_transaction_alert() -> DataFrame:
    """
    Streaming table that generates alerts for high-value transactions.
    
    Join Pattern: Stream-static join
    - Streaming transactions are enriched with customer profile data
    - Each transaction is checked against its customer's limit
    
    Alert Logic:
    - Triggers when: transaction amount > customer transaction_limit
    - Alert includes: Full transaction details + customer contact info
    
    Returns:
        DataFrame: Alert records with transaction and customer details
    """
    # Read streaming transactions from silver layer
    transaction = spark.readStream.table("fraudradar.silver.transactions")
    
    # Read customer dimension table (batch read for stream-static join)
    customer = spark.read.table("fraudradar.silver.customers")

    # Join transactions with customer data and filter for limit breaches
    # Left join ensures all transactions are evaluated even if customer not found
    joined_df = (transaction.join(customer, on="customer_id", how="left")
                 .filter(F.col("amount") > F.col("transaction_limit")))
                         
    # Build alert record with comprehensive transaction and customer details
    final_df = joined_df.select(
        # Alert identifiers
        F.concat_ws("-", F.lit("ALERT"), F.col("transaction_id")).alias("alert_id"),
        F.lit("HIGH_VALUE_TRANSACTION").alias("alert_type"),
        
        # Transaction identifiers
        transaction.transaction_id,
        transaction.customer_id,
        
        # Alert trigger details (amount vs limit)
        transaction.amount.alias("transaction_amount"),
        customer.transaction_limit,
        
        # Customer contact information for notification
        customer.email.alias("customer_email"),
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("customer_name"),
        
        # Transaction details for investigation
        transaction.currency,
        transaction.merchant_name,
        transaction.merchant_category,
        transaction.transaction_type,
        transaction.payment_channel,
        
        # Location information
        transaction.city,
        transaction.country,
        transaction.is_international,
        
        # Timestamps for tracking
        transaction.transaction_timestamp,
        transaction.status,
        F.current_timestamp().alias("alert_ts")  # When alert was generated
    )
    

    return final_df

