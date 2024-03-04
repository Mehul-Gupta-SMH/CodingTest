import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime


config = {
    'sender_email' : "your_email@example.com",
    'receiver_email' : "recipient@example.com",
    'subject' : f"Daily Report : Github Repo Management : Status - {datetime.now().strftime('%Y/%m/%d')}",
    'smtp_server' : "smtp.example.com",
    'smtp_port' : 587,
    'sender_password' : "your_email_password"
}


def generate_report(metrics_dict: dict):
    """
    Generate and send an email report with provided metrics.
    :param metrics_dict: A dictionary containing metrics to include in the report.
    :return: None
    """
    # Create a MIMEText object to represent the email content
    email_message = MIMEMultipart()
    email_message['From'] = config['sender_email']
    email_message['To'] = config['receiver_email']
    email_message['Subject'] = config['subject']

    # Format metrics for inclusion in the email body
    report_metrics = "\n".join([keys + " : " + vals for keys, vals in metrics_dict])

    # Compose the email message
    message = f"""
    
    Below are the updates of {datetime.now().strftime('%Y/%m/%d')}
    
    {report_metrics}
    
    """

    email_message.attach(MIMEText(message, 'plain'))

    # Connect to the SMTP server and send the email
    with smtplib.SMTP(config["smtp_server"], config["smtp_port"]) as server:
        server.starttls()  # Start TLS encryption
        server.login(config["sender_email"], config["sender_password"])
        server.sendmail(config["sender_email"], config["receiver_email"], email_message.as_string())

