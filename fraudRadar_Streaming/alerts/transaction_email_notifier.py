from pyspark import pipelines as dp
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

"""
Alerts Layer: High-Value Transaction Email Notifier

Sends email notifications for high-value transaction alerts using ForEachBatch Sink pattern.

Architecture:
- Source: fraudradar.gold.high_value_transaction_alert (streaming table)
- Sink: ForEachBatch Sink (processes each micro-batch with custom Python logic)
- Destination: External email system

Why ForEachBatch Sink?
ForEachBatch is the correct pattern when:
1. Writing to external systems (not Delta tables)
2. Custom per-record processing needed (email per transaction)
3. Complex logic beyond simple write (HTML generation, error handling)
4. External API/service integration

Alternatives NOT used:
- Regular Sink (dp.create_sink): Only for Delta, Kafka, or Event Hubs
- Streaming table: Cannot call external APIs in dataset definitions
- Stream to table + separate job: Adds latency and complexity
"""

# ========================================
# Configuration
# ========================================

class EmailConfig:
    """Email service configuration"""
    SENDER_EMAIL = "yoursbestie37@gmail.com"  # From address (must match authenticated account)
    SMTP_HOST = "smtp.gmail.com"              # SMTP server
    SMTP_PORT = 587                            # TLS port
    SECRET_SCOPE = "fraudradar-scope"          # Databricks secret scope name
    SECRET_KEY = "gmail_api_key"               # Secret key for authentication


# ========================================
# Email Service
# ========================================

def send_email(to_email, subject, body):
    """
    Send HTML email via Gmail SMTP.
    
    Args:
        to_email: Recipient email address
        subject: Email subject
        body: HTML email body
    
    Returns:
        True if sent successfully, False otherwise
    """
    try:
        # Retrieve secure credentials from Databricks Secrets (never hardcode passwords)
        password = dbutils.secrets.get(scope=EmailConfig.SECRET_SCOPE, key=EmailConfig.SECRET_KEY)
        
        # Build MIME email message with HTML content
        msg = MIMEMultipart()
        msg["From"] = EmailConfig.SENDER_EMAIL   # Sender address
        msg["To"] = to_email                     # Recipient address
        msg["Subject"] = subject                 # Email subject line
        msg.attach(MIMEText(body, "html"))      # Attach HTML body
        
        # Send via SMTP with TLS encryption
        with smtplib.SMTP(EmailConfig.SMTP_HOST, EmailConfig.SMTP_PORT) as server:
            server.starttls()
            server.login(EmailConfig.SENDER_EMAIL, password)
            server.send_message(msg)
        
        print(f"✅ Email sent to {to_email}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to send email to {to_email}: {e}")
        return False


def create_alert_email(data):
    """
    Generate HTML email for high-value transaction alert.
    
    Creates a visually formatted HTML email with:
    - Warning banner styling (yellow background, red text)
    - Structured transaction details in a table
    - Key information: ID, amount, customer, timestamp, location
    
    Args:
        data: Dictionary with transaction details
    
    Returns:
        (subject, body) tuple
    """
    # Email subject with amount in dollars
    subject = f"🚨 High-Value Transaction Alert - ${data.get('amount', 'N/A')}"
    
    # HTML email template with inline CSS for email client compatibility
    body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
        <div style="background: #fff3cd; border: 2px solid #ffc107; padding: 20px; border-radius: 8px;">
            <h2 style="color: #d9534f; margin-top: 0;">⚠️ High-Value Transaction Alert</h2>
            <p>A transaction has exceeded the customer's limit:</p>
            
            <table style="width: 100%; margin: 20px 0; border-collapse: collapse;">
                <tr style="border-bottom: 1px solid #ddd;">
                    <td style="padding: 10px; font-weight: bold;">Transaction ID:</td>
                    <td style="padding: 10px; color: #007bff;">{data.get('transaction_id', 'N/A')}</td>
                </tr>
                <tr style="border-bottom: 1px solid #ddd;">
                    <td style="padding: 10px; font-weight: bold;">Amount:</td>
                    <td style="padding: 10px; color: #d9534f; font-size: 18px;">${data.get('amount', 'N/A')}</td>
                </tr>
                <tr style="border-bottom: 1px solid #ddd;">
                    <td style="padding: 10px; font-weight: bold;">Customer:</td>
                    <td style="padding: 10px;">{data.get('customer_name', 'N/A')}</td>
                </tr>
                <tr style="border-bottom: 1px solid #ddd;">
                    <td style="padding: 10px; font-weight: bold;">Timestamp:</td>
                    <td style="padding: 10px;">{data.get('timestamp', 'N/A')}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; font-weight: bold;">Location:</td>
                    <td style="padding: 10px;">{data.get('location', 'N/A')}</td>
                </tr>
            </table>
            
            <p style="font-size: 12px; color: #666; margin-top: 20px;">
                Automated alert from FraudRadar Streaming System
            </p>
        </div>
    </body>
    </html>
    """
    
    return subject, body


# ========================================
# Streaming Sink
# ========================================

@dp.foreach_batch_sink(name="high_value_transaction_email_sink")
def send_transaction_alerts(df, batch_id):
    """
    ForEachBatch Sink: Send email alert for each high-value transaction.
    
    This function is called once per micro-batch with the entire batch DataFrame.
    It processes each row individually to send one email per transaction.
    
    Batch Processing:
    - Collects entire batch to driver (acceptable for alert volumes)
    - Iterates through transactions sequentially
    - Continues on individual failures (resilient processing)
    - Reports summary metrics per batch
    
    Args:
        df: Spark DataFrame with transaction alerts from gold layer
        batch_id: Unique micro-batch identifier (for logging)
    """
    print(f"📧 Processing batch {batch_id}...")
    
    # Collect batch data to driver for iteration
    # Note: .collect() brings all data to driver - acceptable for alert volumes
    transactions = [row.asDict() for row in df.collect()]
    
    # Early return for empty batches (normal in streaming)
    if not transactions:
        print(f"ℹ️ Batch {batch_id}: No transactions")
        return
    
    print(f"📨 Batch {batch_id}: {len(transactions)} transaction(s)")
    
    # Track success/failure counts for batch summary
    success = 0
    failed = 0
    
    # Iterate through each transaction in the batch
    for txn in transactions:
        # Validate email address exists (may be NULL from left join in gold layer)
        email = txn.get('customer_email')
        if not email:
            print(f"⚠️ No email for transaction {txn.get('transaction_id', 'unknown')}")
            failed += 1
            continue  # Skip this transaction, continue with next
        
        # Extract relevant fields for email content
        email_data = {
            'transaction_id': txn.get('transaction_id', 'N/A'),
            'amount': txn.get('transaction_amount', 'N/A'),
            'customer_name': txn.get('customer_name', 'N/A'),
            'timestamp': str(txn.get('transaction_timestamp', 'N/A')),
            'location': f"{txn.get('city', 'N/A')}, {txn.get('country', 'N/A')}"
        }
        
        # Generate HTML email and send
        # Individual failures don't block remaining messages
        subject, body = create_alert_email(email_data)
        if send_email(email, subject, body):
            success += 1
        else:
            failed += 1  # Error already logged in send_email()
    
    print(f"✅ Batch {batch_id} complete: {success} sent, {failed} failed")


# ========================================
# Flow Definition
# ========================================

@dp.append_flow(target="high_value_transaction_email_sink")
def high_value_transaction_alert_stream():
    """
    Append Flow: Stream high-value transaction alerts to email sink.
    
    Connects the gold layer streaming table to the ForEachBatch Sink.
    
    Data Flow:
    1. Read streaming alerts from gold.high_value_transaction_alert
    2. Stream to ForEachBatch Sink (high_value_transaction_email_sink)
    3. Sink processes each micro-batch and sends emails
    
    Why Append Flow?
    - ForEachBatch Sinks require an Append Flow to connect the source
    - Each alert generates exactly one email (append-only semantics)
    - No updates or deletes needed (one-way notification)
    """
    return spark.readStream.table("fraudradar.gold.high_value_transaction_alert")