import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import re


def is_valid_email(email):
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    return re.match(pattern, email) is not None


def send_authentication_email(recipient_email, authentication_link):
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = os.getenv("SMTP_PORT")
    smtp_username = os.getenv("SMTP_USERNAME")
    smtp_password = os.getenv("SMTP_PASSWORD")
    sender_email = os.getenv("SENDER_EMAIL")

    # Create the email message
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = recipient_email
    message["Subject"] = "Email Authentication"

    # Email body
    body = f"""
    Dear User,

    Thank you for registering. Please click the following link to authenticate your email:

    {authentication_link}

    If you did not register, please ignore this email.

    Best regards,
    Bagels
    """

    message.attach(MIMEText(body, "plain"))

    try:
        # Create a secure SSL/TLS connection
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_username, smtp_password)

        # Send the email
        server.send_message(message)

        print(f"Authentication email sent to {recipient_email}")
    except Exception as e:
        print(f"Error sending email: {str(e)}")
    finally:
        server.quit()
