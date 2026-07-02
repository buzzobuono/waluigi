"""
SendEmail — send an email via any SMTP server (generic, non-vendor).

Connection settings (non-sensitive) come from ``config.smtp``:
    host      str   Required — SMTP server hostname (e.g. smtp.example.com)
    port      int   Required — SMTP port (e.g. 465 for SSL, 587 for STARTTLS)
    security  str   ssl | starttls | none   (default: starttls)
    user      str   SMTP username (optional — omit for unauthenticated relay)
    from      str   Sender address (default: user)

The password (sensitive) is read from a Waluigi Secret:
    WALUIGI_SECRET_SMTP_PASSWORD   SMTP password / app password

Login is attempted only when both ``user`` and the password secret are present,
so unauthenticated internal relays are supported.

Message content comes from params (same names as SendGmail, so JobHook
``${event.*}`` params stay compatible):
    to      str   recipient address(es), comma-separated
    subject str   email subject
    body    str   email body
    body_type   plain | html  (default: plain)
    cc          str           CC address(es), comma-separated
    bcc         str           BCC address(es), comma-separated

Fallback: if ``to`` is not set as a param, WALUIGI_SECRET_SMTP_NOTIFY_TO is
used — useful for recurring notification jobs where the recipient is fixed.
"""
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from waluigi.sdk.context import context


def run():
    smtp_cfg = context.config.get("smtp") or {}

    host     = smtp_cfg.get("host")
    port     = smtp_cfg.get("port")
    security = (smtp_cfg.get("security") or "starttls").lower()
    user     = smtp_cfg.get("user") or ""
    sender   = smtp_cfg.get("from") or user
    password = os.environ.get("WALUIGI_SECRET_SMTP_PASSWORD", "")

    if not host:
        raise ValueError("config.smtp.host is required")
    if not port:
        raise ValueError("config.smtp.port is required")
    if security not in ("ssl", "starttls", "none"):
        raise ValueError(f"config.smtp.security must be 'ssl', 'starttls' or 'none', got '{security}'")
    if not sender:
        raise ValueError("config.smtp.from (or config.smtp.user) is required")
    port = int(port)

    to        = getattr(context.params, "to",        None) or os.environ.get("WALUIGI_SECRET_SMTP_NOTIFY_TO", "")
    subject   = getattr(context.params, "subject",   "Notifica Waluigi")
    body      = getattr(context.params, "body",      "")
    body_type = getattr(context.params, "body_type", "plain")
    cc        = getattr(context.params, "cc",        "")
    bcc       = getattr(context.params, "bcc",       "")

    if not to:
        raise ValueError("param 'to' required (or set secret SMTP_NOTIFY_TO as default recipient)")
    if not body:
        raise ValueError("param 'body' required")
    if body_type not in ("plain", "html"):
        raise ValueError(f"body_type must be 'plain' or 'html', got '{body_type}'")

    msg = MIMEMultipart("alternative")
    msg["From"]    = sender
    msg["To"]      = to
    msg["Subject"] = subject
    if cc:
        msg["Cc"] = cc
    if bcc:
        msg["Bcc"] = bcc

    msg.attach(MIMEText(body, body_type, "utf-8"))

    all_recipients = [a.strip() for a in (to + "," + cc + "," + bcc).split(",") if a.strip()]

    if security == "ssl":
        smtp = smtplib.SMTP_SSL(host, port)
    else:
        smtp = smtplib.SMTP(host, port)
    with smtp:
        if security == "starttls":
            smtp.starttls()
        if user and password:
            smtp.login(user, password)
        smtp.sendmail(sender, all_recipients, msg.as_string())

    print(f"Mail inviata → {to} | oggetto: {subject} | via {host}:{port} ({security})")


if __name__ == "__main__":
    run()
