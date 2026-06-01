"""Shared flow notification hooks - send email on completion or failure."""

import smtplib
from email.message import EmailMessage
from datetime import datetime
from config_utils import resolve


def _send_email(subject: str, body: str):
    email_from = resolve('alert-email-from', 'ALERT_EMAIL_FROM')
    email_to = resolve('alert-email-to', 'ALERT_EMAIL_TO')
    app_password = resolve('gmail-app-password', 'GMAIL_APP_PASSWORD', is_secret=True)

    if not all([email_from, email_to, app_password]):
        print("[WARN] Email not configured - skipping notification")
        return

    msg = EmailMessage()
    msg["From"] = email_from
    msg["To"] = email_to
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(email_from, app_password)
            server.send_message(msg)
        print(f"[OK] Notification sent to {email_to}")
    except smtplib.SMTPAuthenticationError:
        print("[ERROR] Gmail auth failed - check GMAIL_APP_PASSWORD")
    except Exception as e:
        print(f"[ERROR] Failed to send notification: {e}")


def on_flow_complete(flow, flow_run, state):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    duration = ""
    if flow_run.total_run_time:
        mins = int(flow_run.total_run_time.total_seconds() // 60)
        secs = int(flow_run.total_run_time.total_seconds() % 60)
        duration = f"\nDuration: {mins}m {secs}s"

    _send_email(
        subject=f"[OK] {flow.name} completed",
        body=(
            f"Flow: {flow.name}\n"
            f"State: {state.type}\n"
            f"Time: {now}{duration}\n"
            f"Run ID: {flow_run.id}"
        ),
    )


def on_flow_failure(flow, flow_run, state):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    error_msg = str(state.result()) if state.result() else "Unknown error"

    _send_email(
        subject=f"[ERROR] {flow.name} failed",
        body=(
            f"Flow: {flow.name}\n"
            f"State: {state.type}\n"
            f"Time: {now}\n"
            f"Error: {error_msg}\n"
            f"Run ID: {flow_run.id}"
        ),
    )
