"""
SendGmail — send an email via Gmail SMTP using an App Password.

Credentials are read from Waluigi Secrets:
    GMAIL_USER          sender address (e.g. me@gmail.com)
    GMAIL_APP_PASSWORD  16-char App Password from Google Account → Security

Required params:
    to      str   recipient address(es), comma-separated
    subject str   email subject
    body    str   email body

Optional params:
    body_type   plain | html  (default: plain)
    cc          str           CC address(es), comma-separated
    bcc         str           BCC address(es), comma-separated

Fallback: if `to` is not set as a param, WALUIGI_SECRET_GMAIL_NOTIFY_TO
is used — useful for recurring notification jobs where the recipient is fixed.
"""
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from waluigi.sdk.context import context


def run():
    gmail_user     = os.environ["WALUIGI_SECRET_GMAIL_USER"]
    gmail_password = os.environ["WALUIGI_SECRET_GMAIL_APP_PASSWORD"]

    to        = getattr(context.params, "to",        None) or os.environ.get("WALUIGI_SECRET_GMAIL_NOTIFY_TO", "")
    subject   = getattr(context.params, "subject",   "Notifica Waluigi")
    body      = getattr(context.params, "body",      "")
    body_type = getattr(context.params, "body_type", "plain")
    cc        = getattr(context.params, "cc",        "")
    bcc       = getattr(context.params, "bcc",       "")

    if not to:
        raise ValueError("param 'to' required (or set secret GMAIL_NOTIFY_TO as default recipient)")
    if not body:
        raise ValueError("param 'body' required")
    if body_type not in ("plain", "html"):
        raise ValueError(f"body_type must be 'plain' or 'html', got '{body_type}'")

    msg = MIMEMultipart("alternative")
    msg["From"]    = gmail_user
    msg["To"]      = to
    msg["Subject"] = subject
    if cc:
        msg["Cc"] = cc
    if bcc:
        msg["Bcc"] = bcc

    msg.attach(MIMEText(body, body_type, "utf-8"))

    all_recipients = [a.strip() for a in (to + "," + cc + "," + bcc).split(",") if a.strip()]

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(gmail_user, gmail_password)
        smtp.sendmail(gmail_user, all_recipients, msg.as_string())

    print(f"Mail inviata → {to} | oggetto: {subject}")


if __name__ == "__main__":
    run()
