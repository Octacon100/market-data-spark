"""
Test Gmail SMTP email sending.
Verifies your Gmail App Password works before wiring into Prefect flows.
"""

import smtplib
import os
from email.message import EmailMessage
from dotenv import load_dotenv

load_dotenv()


def send_test_email():
    email_from = os.getenv('ALERT_EMAIL_FROM')
    email_to = os.getenv('ALERT_EMAIL_TO')
    app_password = os.getenv('GMAIL_APP_PASSWORD')

    if not all([email_from, email_to, app_password]):
        print("[ERROR] Missing .env variables. Need: ALERT_EMAIL_FROM, ALERT_EMAIL_TO, GMAIL_APP_PASSWORD")
        return

    msg = EmailMessage()
    msg['From'] = email_from
    msg['To'] = email_to
    msg['Subject'] = 'Market Data Pipeline - Gmail SMTP Test'
    msg.set_content(
        'This is a test email from your Market Data Pipeline.\n\n'
        'Gmail SMTP is configured and working.\n'
        'Buy signal alerts will be sent from this address.'
    )

    print(f"[INFO] Sending test email from {email_from} to {email_to}...")

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(email_from, app_password)
            server.send_message(msg)
        print("[OK] Test email sent successfully! Check your inbox.")
    except smtplib.SMTPAuthenticationError:
        print("[ERROR] Authentication failed. Check your GMAIL_APP_PASSWORD in .env")
        print("[INFO] Generate an app password at: myaccount.google.com > Security > App passwords")
    except Exception as e:
        print(f"[ERROR] Failed to send email: {e}")


if __name__ == "__main__":
    send_test_email()
