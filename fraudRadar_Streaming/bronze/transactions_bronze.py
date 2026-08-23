from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import json

"""
Bronze Layer: Transactions from Kafka

Ingests raw streaming transaction events from Kafka into the bronze layer.
Implements:
- Kafka streaming source with SASL_SSL authentication
- Secure credential management via Databricks Secrets
- Complete preservation of Kafka metadata for lineage and replay
- Schema-on-read with string casting (parsing deferred to silver layer)

Bronze layer philosophy:
- Preserve raw data exactly as received from source
- Minimal transformation (only type casting for compatibility)
- Keep all source metadata (topic, partition, offset, timestamp)
- Enable future reprocessing and debugging
"""

@dp.table(
    name="fraudradar.bronze.transactions",
    comment="Raw stream data from Kafka for transactions"
)
def transactions_bronze() -> DataFrame:
    """
    Streaming table that ingests raw transaction events from Kafka.
    
    Authentication: SASL_SSL with PLAIN mechanism
    - Credentials stored securely in Databricks Secrets
    - Uses shaded Kafka classes (kafkashaded.*) to avoid conflicts
    
    Data Strategy:
    - Preserves all Kafka metadata (topic, partition, offset, timestamp)
    - Minimal transformation: cast to string for compatibility
    - Actual JSON parsing deferred to silver layer
    - startingOffsets="earliest" for complete history on first run
    
    Returns:
        DataFrame: Raw Kafka records with metadata columns
    """
    # ========================================
    # Retrieve Kafka Connection Details
    # ========================================
    # Fetch credentials from Databricks Secrets (secure key-value store)
    # Never hardcode credentials in pipeline code
    kafka_connection_json = dbutils.secrets.get(scope="fraudradar-scope",key="kafka_connection_details")
    kafka_config = json.loads(kafka_connection_json)
    
    # Extract connection parameters from JSON configuration
    bootstrap_server=kafka_config['bootstrap_servers']  # Kafka broker addresses
    api_key=kafka_config['api_key']                     # SASL username
    api_secret=kafka_config['api_secret']               # SASL password
    topic_name=kafka_config['topic']                    # Topic to subscribe to

    # ========================================
    # Build JAAS Configuration for SASL Auth
    # ========================================
    # JAAS (Java Authentication and Authorization Service) config for SASL PLAIN
    # CRITICAL: Must use 'kafkashaded.*' prefix (not 'org.apache.kafka.*')
    # The shaded classes prevent conflicts with Spark's bundled Kafka client
    jaas_config=f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'

    # ========================================
    # Read Streaming Data from Kafka
    # ========================================
    # Configure Kafka streaming source with authentication
    sample_streaming_data = (spark.readStream.format("kafka")
                     .option("kafka.bootstrap.servers", bootstrap_server)  # Kafka cluster address
                     .option("subscribe", topic_name)                      # Topic to consume from
                     
                     # Security configuration (SASL over SSL)
                     .option("kafka.security.protocol", "SASL_SSL")       # Encrypted connection
                     .option("kafka.sasl.mechanism", "PLAIN")             # SASL PLAIN authentication
                     .option("kafka.sasl.jaas.config", jaas_config)       # Credentials via JAAS
                     
                     # Starting position for first run ("earliest" = from beginning, "latest" = only new)
                     .option("startingOffsets","earliest")
                     .load())
    
    # ========================================
    # Cast and Preserve Kafka Metadata
    # ========================================
    # Bronze layer preserves raw data with minimal transformation
    parsed_streaming_data = sample_streaming_data.select(
                            # Cast key and value to string for downstream processing
                            col("key").cast("string"),              # Message key (often null for non-keyed topics)
                            col("value").cast("string"),            # Message payload (JSON string, parsed in silver)
                            
                            # Preserve all Kafka metadata for lineage and debugging
                            col("topic"),                           # Source topic name
                            col("partition"),                       # Partition number (for ordering within partition)
                            col("offset"),                          # Offset within partition (unique position)
                            col("timestamp"),                       # Kafka event timestamp (broker or producer time)
                            col("timestampType"),                   # Type of timestamp (CreateTime vs LogAppendTime)
                            
                            # Add ingestion timestamp for data lineage
                            F.current_timestamp().alias("ingesttime"))  # When this record was ingested into bronze
    
    return parsed_streaming_data
    
    




