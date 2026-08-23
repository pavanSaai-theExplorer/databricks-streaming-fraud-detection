from pyspark import pipelines as dp
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

"""
Alerts Layer: Fraud Card Alert Email Notifier

Sends email notifications for fraud card alerts using ForEachBatch Sink pattern.

Architecture:
- Source: fraudradar.gold.fraud_card_alert (streaming table from stream-stream join)
- Sink: ForEachBatch Sink (processes each micro-batch with custom Python logic)
- Destination: External email system

Alert Content:
Richer than high-value alerts - includes:
- Complete transaction details (amount, merchant, location, timestamp)
- Fraud watchlist match details (risk level, reason, reporter, effective date)
- Risk-based color coding (CRITICAL=red, HIGH=orange, MEDIUM=yellow, LOW=green)
- Recommended action (BLOCK, REVIEW, MONITOR)

Why ForEachBatch Sink?
- Custom HTML generation with dynamic risk-level styling
- External system integration (email service)
- Per-record processing with individual error handling
- Cannot use regular streaming table (no external API calls in dataset definitions)
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


def create_fraud_alert_email(data):
    """
    Generate comprehensive HTML email for fraud card alert.
    
    Args:
        data: Dictionary with fraud alert, transaction, and watchlist details
    
    Returns:
        (subject, body) tuple
    """
    risk_level = data.get('risk_level', 'UNKNOWN')
    subject = f"🚨 FRAUD ALERT - {risk_level} Risk - Card {data.get('card_number', 'N/A')}"
    
    risk_colors = {
        'CRITICAL': '#dc3545',
        'HIGH': '#fd7e14',
        'MEDIUM': '#ffc107',
        'LOW': '#28a745',
        'UNKNOWN': '#6c757d'
    }
    risk_color = risk_colors.get(risk_level, '#6c757d')
    
    body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px; background-color: #f8f9fa;">
        <div style="background: #ffffff; border: 3px solid {risk_color}; padding: 30px; border-radius: 10px; max-width: 800px; margin: 0 auto;">
            
            <h2 style="color: {risk_color}; margin-top: 0; border-bottom: 2px solid {risk_color}; padding-bottom: 10px;">
                🚨 FRAUD WATCHLIST ALERT
            </h2>
            
            <div style="background: #fff3cd; padding: 15px; border-radius: 5px; margin: 20px 0; border-left: 4px solid {risk_color};">
                <h3 style="margin: 0 0 10px 0; color: #856404;">Alert Summary</h3>
                <p style="margin: 5px 0; font-size: 14px;">
                    <strong>Risk Level:</strong> <span style="color: {risk_color}; font-weight: bold; font-size: 16px;">{risk_level}</span>
                </p>
                <p style="margin: 5px 0; font-size: 14px;">
                    <strong>Action Required:</strong> <span style="color: {risk_color}; font-weight: bold;">{data.get('action', 'N/A')}</span>
                </p>
                <p style="margin: 5px 0; font-size: 14px;">
                    <strong>Alert ID:</strong> {data.get('alert_id', 'N/A')}
                </p>
                <p style="margin: 5px 0; font-size: 14px;">
                    <strong>Alert Time:</strong> {data.get('alert_timestamp', 'N/A')}
                </p>
            </div>
            
            <h3 style="color: #343a40; border-bottom: 1px solid #dee2e6; padding-bottom: 8px;">Transaction Details</h3>
            <table style="width: 100%; margin: 15px 0; border-collapse: collapse;">
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <td style="padding: 10px; font-weight: bold; width: 40%;">Transaction ID:</td>
                    <td style="padding: 10px; color: #007bff;">{data.get('transaction_id', 'N/A')}</td>
                </tr>
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <td style="padding: 10px; font-weight: bold;">Customer:</td>
                    <td style="padding: 10px;">{data.get('customer_name', 'N/A')}</td>
                </tr>
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <td style="padding: 10px; font-weight: bold;">Card Number:</td>
                    <td style="padding: 10px; color: #dc3545; font-weight: bold;">{data.get('card_number', 'N/A')}</td>
                </tr>
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <td style="padding: 10px; font-weight: bold;">Amount:</td>
                    <td style="padding: 10px; font-size: 16px; color: #28a745; font-weight: bold;">{data.get('amount', 'N/A')} {data.get('currency', '')}</td>
                </tr>
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <td style="padding: 10px; font-weight: bold;">Merchant:</td>
                    <td style="padding: 10px;">{data.get('merchant_name', 'N/A')}</td>
                </tr>
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <td style="padding: 10px; font-weight: bold;">Merchant Category:</td>
                    <td style="padding: 10px;">{data.get('merchant_category', 'N/A')}</td>
                </tr>
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <td style="padding: 10px; font-weight: bold;">Transaction Type:</td>
                    <td style="padding: 10px;">{data.get('transaction_type', 'N/A')}</td>
                </tr>
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <td style="padding: 10px; font-weight: bold;">Payment Channel:</td>
                    <td style="padding: 10px;">{data.get('payment_channel', 'N/A')}</td>
                </tr>
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <td style="padding: 10px; font-weight: bold;">Location:</td>
                    <td style="padding: 10px;">{data.get('transaction_location', 'N/A')}</td>
                </tr>
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <td style="padding: 10px; font-weight: bold;">Timestamp:</td>
                    <td style="padding: 10px;">{data.get('transaction_timestamp', 'N/A')}</td>
                </tr>
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <td style="padding: 10px; font-weight: bold;">International:</td>
                    <td style="padding: 10px;">{data.get('is_international', 'N/A')}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; font-weight: bold;">Status:</td>
                    <td style="padding: 10px;">{data.get('transaction_status', 'N/A')}</td>
                </tr>
            </table>
            
            <h3 style="color: #dc3545; border-bottom: 1px solid #dee2e6; padding-bottom: 8px; margin-top: 30px;">🔍 Fraud Watchlist Match</h3>
            <table style="width: 100%; margin: 15px 0; border-collapse: collapse; background: #fff5f5;">
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <td style="padding: 10px; font-weight: bold; width: 40%;">Watchlist ID:</td>
                    <td style="padding: 10px; color: #dc3545; font-weight: bold;">{data.get('watchlist_id', 'N/A')}</td>
                </tr>
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <td style="padding: 10px; font-weight: bold;">Watch Type:</td>
                    <td style="padding: 10px;">{data.get('watch_type', 'N/A')}</td>
                </tr>
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <td style="padding: 10px; font-weight: bold;">Risk Level:</td>
                    <td style="padding: 10px; color: {risk_color}; font-weight: bold; font-size: 16px;">{risk_level}</td>
                </tr>
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <td style="padding: 10px; font-weight: bold;">Reason Code:</td>
                    <td style="padding: 10px;">{data.get('reason_code', 'N/A')}</td>
                </tr>
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <td style="padding: 10px; font-weight: bold;">Reason:</td>
                    <td style="padding: 10px;">{data.get('reason_description', 'N/A')}</td>
                </tr>
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <td style="padding: 10px; font-weight: bold;">Reported By:</td>
                    <td style="padding: 10px;">{data.get('reported_by', 'N/A')}</td>
                </tr>
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <td style="padding: 10px; font-weight: bold;">Reported Source:</td>
                    <td style="padding: 10px;">{data.get('reported_source', 'N/A')}</td>
                </tr>
                <tr style="border-bottom: 1px solid #dee2e6;">
                    <td style="padding: 10px; font-weight: bold;">Effective From:</td>
                    <td style="padding: 10px;">{data.get('watchlist_effective_from', 'N/A')}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; font-weight: bold;">Watchlist Location:</td>
                    <td style="padding: 10px;">{data.get('watchlist_location', 'N/A')}</td>
                </tr>
            </table>
            
            <div style="margin-top: 30px; padding-top: 20px; border-top: 1px solid #dee2e6;">
                <p style="font-size: 12px; color: #6c757d; margin: 5px 0;">
                    <strong>Important:</strong> This is an automated fraud alert from the FraudRadar Streaming System.
                </p>
                <p style="font-size: 12px; color: #6c757d; margin: 5px 0;">
                    Please review this transaction immediately and take appropriate action as indicated.
                </p>
                <p style="font-size: 11px; color: #adb5bd; margin: 15px 0 0 0;">
                    Alert Type: {data.get('alert_type', 'N/A')} | Generated: {data.get('alert_timestamp', 'N/A')}
                </p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return subject, body


# ========================================
# Streaming Sink
# ========================================

@dp.foreach_batch_sink(name="fraud_card_alert_email_sink")
def send_fraud_card_alerts(df, batch_id):
    """
    Send email alert for each fraud card alert in the batch.
    
    Args:
        df: Spark DataFrame with fraud card alerts
        batch_id: Micro-batch ID
    """
    print(f"📧 Processing fraud alert batch {batch_id}...")
    
    alerts = [row.asDict() for row in df.collect()]
    
    if not alerts:
        print(f"ℹ️ Batch {batch_id}: No fraud alerts")
        return
    
    print(f"🚨 Batch {batch_id}: {len(alerts)} fraud alert(s)")
    
    success = 0
    failed = 0
    
    for alert in alerts:
        email = alert.get('customer_email')
        if not email:
            print(f"⚠️ No email for alert {alert.get('alert_id', 'unknown')}")
            failed += 1
            continue
        
        # Build email data dictionary from alert fields
        email_data = {
            'alert_id': alert.get('alert_id', 'N/A'),
            'alert_type': alert.get('alert_type', 'N/A'),
            'alert_timestamp': str(alert.get('alert_timestamp', 'N/A')),
            'transaction_id': alert.get('transaction_id', 'N/A'),
            'customer_name': alert.get('customer_name', 'N/A'),
            'card_number': alert.get('card_number', 'N/A'),
            'amount': alert.get('amount', 'N/A'),
            'currency': alert.get('currency', 'N/A'),
            'merchant_name': alert.get('merchant_name', 'N/A'),
            'merchant_category': alert.get('merchant_category', 'N/A'),
            'transaction_type': alert.get('transaction_type', 'N/A'),
            'payment_channel': alert.get('payment_channel', 'N/A'),
            'transaction_location': f"{alert.get('transaction_city', 'N/A')}, {alert.get('transaction_country', 'N/A')}",
            'transaction_timestamp': str(alert.get('transaction_timestamp', 'N/A')),
            'is_international': alert.get('is_international', 'N/A'),
            'transaction_status': alert.get('transaction_status', 'N/A'),
            'watchlist_id': alert.get('watchlist_id', 'N/A'),
            'watch_type': alert.get('watch_type', 'N/A'),
            'risk_level': alert.get('risk_level', 'N/A'),
            'action': alert.get('action', 'N/A'),
            'reason_code': alert.get('reason_code', 'N/A'),
            'reason_description': alert.get('reason_description', 'N/A'),
            'reported_by': alert.get('reported_by', 'N/A'),
            'reported_source': alert.get('reported_source', 'N/A'),
            'watchlist_effective_from': str(alert.get('watchlist_effective_from', 'N/A')),
            'watchlist_location': f"{alert.get('watchlist_city', 'N/A')}, {alert.get('watchlist_country', 'N/A')}"
        }
        
        subject, body = create_fraud_alert_email(email_data)
        if send_email(email, subject, body):
            success += 1
        else:
            failed += 1
    
    print(f"✅ Batch {batch_id} complete: {success} sent, {failed} failed")


# ========================================
# Flow Definition
# ========================================

@dp.append_flow(target="fraud_card_alert_email_sink")
def fraud_card_alert_stream():
    """Stream fraud card alerts to email sink"""
    return spark.readStream.table("fraudradar.gold.fraud_card_alert")