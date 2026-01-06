from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage


def _load_dotenv_if_present() -> None:
    """
    Load .env if python-dotenv is installed.
    Safe no-op if not installed.
    """
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return
    load_dotenv(override=False)


def send_csv_email(csv_path: str, subject: str, body: str) -> None:
    _load_dotenv_if_present()

    smtp_host = os.getenv("SMTP_HOST", "").strip()
    smtp_port_raw = os.getenv("SMTP_PORT", "").strip()
    smtp_user = os.getenv("SMTP_USER", "").strip()
    smtp_pass = os.getenv("SMTP_PASS", "").strip()
    email_to = os.getenv("EMAIL_TO", "").strip()
    email_from = os.getenv("SMTP_FROM", smtp_user).strip()

    if not smtp_port_raw:
        # Default to STARTTLS port (most common)
        smtp_port = 587
    else:
        try:
            smtp_port = int(smtp_port_raw)
        except ValueError as e:
            raise RuntimeError(f"SMTP_PORT must be an integer, got: {smtp_port_raw!r}") from e

    if not (smtp_host and smtp_user and smtp_pass and email_to):
        raise RuntimeError(
            "Missing SMTP config. Check .env: SMTP_HOST/SMTP_PORT/SMTP_USER/SMTP_PASS/EMAIL_TO"
        )

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = email_to
    msg.set_content(body)

    filename = os.path.basename(csv_path)
    with open(csv_path, "rb") as f:
        msg.add_attachment(f.read(), maintype="text", subtype="csv", filename=filename)

    # --- Connect ---
    if smtp_port == 465:
        # Implicit SSL
        with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
    else:
        # STARTTLS (587 recommended)
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
