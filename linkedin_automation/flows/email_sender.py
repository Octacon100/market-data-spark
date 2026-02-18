"""
Email delivery task for LinkedIn post drafts.

Uses smtplib from the standard library so there is no additional runtime
dependency. Gmail App Passwords are the recommended credential when the
sender account has 2FA enabled - a plain password will fail if "Less secure
app access" is disabled (which it is by default on modern Gmail accounts).
"""

import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List

from prefect import task
from prefect.events import emit_event


@task(
    name="send-draft-email",
    retries=2,
    retry_delay_seconds=30,
    log_prints=True,
    tags=["email", "notification"],
)
def send_draft_email(
    topic: str,
    drafts: List[str],
    smtp_host: str,
    smtp_port: int,
    email_sender: str,
    email_password: str,
    email_recipient: str,
) -> bool:
    """
    Format both draft variations and deliver them via SMTP STARTTLS.

    Two drafts are formatted side-by-side so the recipient can copy-paste
    directly from the email without any additional editing workflow. Character
    counts are included because LinkedIn truncates posts silently at ~3,000
    characters but engagement drops sharply above 1,300.

    Two retries cover transient SMTP timeouts without hammering the server
    if the credential itself is wrong (SMTPAuthenticationError propagates
    immediately without retrying).

    Args:
        topic: The topic string used to generate the drafts.
        drafts: List of two draft post strings produced by the AI.
        smtp_host: SMTP relay hostname (e.g. smtp.gmail.com).
        smtp_port: SMTP port - 587 for STARTTLS, 465 for implicit TLS.
        email_sender: From address; also used as the SMTP login username.
        email_password: App password or SMTP credential for the sender account.
        email_recipient: Destination address for the draft email.

    Returns:
        True on successful delivery.

    Raises:
        smtplib.SMTPAuthenticationError: Bad credentials - will not retry.
        smtplib.SMTPException: Other SMTP failures - will retry per task config.
    """
    today = datetime.now().strftime("%A, %B %d, %Y")
    # Truncate topic in subject so it stays within typical preview lengths
    subject = f"[LinkedIn Draft] {today} - {topic[:55]}"

    draft_1 = drafts[0] if len(drafts) > 0 else "[No draft generated]"
    draft_2 = drafts[1] if len(drafts) > 1 else "[No second draft generated]"

    separator = "=" * 60

    body_lines: List[str] = [
        "LinkedIn Post Drafts - Ready to Copy/Paste",
        separator,
        f"Topic   : {topic}",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        separator,
        "DRAFT 1",
        separator,
        "",
        draft_1,
        "",
        separator,
        "DRAFT 2",
        separator,
        "",
        draft_2,
        "",
        separator,
        "Character counts (LinkedIn sweet spot is under 1,300):",
        f"  Draft 1: {len(draft_1)} chars",
        f"  Draft 2: {len(draft_2)} chars",
        separator,
    ]

    body = "\n".join(body_lines)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = email_sender
    msg["To"] = email_recipient
    msg.attach(MIMEText(body, "plain", "utf-8"))

    print(f"[INFO] Connecting to {smtp_host}:{smtp_port} as {email_sender}")

    # SMTPAuthenticationError is not caught here - let it propagate so Prefect
    # does not burn retry budget on a credential problem that requires human
    # intervention rather than a transient network issue.
    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(email_sender, email_password)
            server.sendmail(email_sender, [email_recipient], msg.as_string())

        print(f"[OK] Draft email delivered to {email_recipient}")
        print(f"[INFO] Subject: {subject}")

        emit_event(
            event="linkedin.draft.email.sent",
            resource={
                "prefect.resource.id": "linkedin-automation-email",
                "prefect.resource.name": "LinkedIn Draft Email Sender",
            },
            payload={
                "topic": topic,
                "recipient": email_recipient,
                "draft_count": len(drafts),
                "draft_1_chars": len(draft_1),
                "draft_2_chars": len(draft_2),
            },
        )

        return True

    except smtplib.SMTPException as exc:
        print(f"[ERROR] SMTP delivery failed: {exc}")
        raise
