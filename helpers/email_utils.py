import os
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


def is_valid_email(email):
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    return re.match(pattern, email) is not None


def send_authentication_email(recipient_email, authentication_link):
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = int(os.getenv("SMTP_PORT"))
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
        # Create a secure TLS connection
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # Upgrade the connection to a secure encrypted TLS connection
        server.login(smtp_username, smtp_password)

        # Send the email
        server.sendmail(sender_email, recipient_email, message.as_string())

        print(f"Authentication email sent to {recipient_email}")
    except Exception as e:
        print(f"Error sending email: {str(e)}")
    finally:
        server.quit()


# Example usage
# send_authentication_email("elijahanderson96@gmail.com", "https://example.com/authenticate")

def send_validation_email(dataframes, recipient_email, image_paths=None):
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = int(os.getenv("SMTP_PORT"))
    smtp_username = os.getenv("SMTP_USERNAME")
    smtp_password = os.getenv("SMTP_PASSWORD")
    sender_email = os.getenv("SENDER_EMAIL")

    # Create the email message
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = recipient_email
    message["Subject"] = "Data Validation Required"

    # Email body
    body = """
    Dear Validator,

    Please find attached the data files and annotated images that need validation. You can edit the CSV files and
    upload them back using the validation link below.

    Validation Link: http://your_fastapi_server_ip:8000/validate

    Best regards,
    Bagels Team
    """
    message.attach(MIMEText(body, "plain"))

    # Attach each dataframe as a CSV file
    for table_name, dataframe in dataframes.items():
        filename = f"{table_name}_validation.csv"
        csv_data = dataframe.to_csv(index=False)
        part = MIMEBase("application", "octet-stream")
        part.set_payload(csv_data.encode('utf-8'))
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={filename}")
        message.attach(part)

    # Attach annotated images if available
    if image_paths:
        for image_path in image_paths:
            with open(image_path, "rb") as image_file:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(image_file.read())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(image_path)}")
                message.attach(part)

    try:
        # Create a secure TLS connection
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # Upgrade the connection to a secure encrypted TLS connection
        server.login(smtp_username, smtp_password)

        # Send the email
        server.sendmail(sender_email, recipient_email, message.as_string())

        print(f"Validation email sent to {recipient_email}")
    except Exception as e:
        print(f"Error sending email: {str(e)}")
    finally:
        server.quit()
