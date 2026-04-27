"""Outreach engine — sends personalized emails via Gmail SMTP.

Tracking strategy:
  - Each link is wrapped with /t/click/<lead_id>/<touch>?u=<encoded_url>
  - Each email gets an open-tracking pixel at /t/open/<lead_id>/<touch>.gif
  - Both endpoints are served by the tracking module (Apify Web Server)
"""
from __future__ import annotations

import asyncio
import smtplib
import ssl
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Any
from urllib.parse import quote_plus

from apify import Actor

from .templates import TEMPLATES

GMAIL_SMTP_HOST = "smtp.gmail.com"
GMAIL_SMTP_PORT = 465
SEND_RATE_PER_MIN = 20  # Gmail free tier safe rate


def _build_tracking_urls(lead: dict[str, Any], touch: int, cfg: dict[str, Any]) -> dict[str, str]:
    base = (cfg.get("trackingBaseUrl") or "").rstrip("/")
    summit_url = cfg.get("summitUrl", "https://swampfoxagency.com/the-summit/")
    if not base:
        # Tracking disabled — fall back to direct links
        return {
            "_track_url": summit_url,
            "_unsub_url": f"{summit_url}#unsubscribe",
            "_pixel_url": "",
        }
    lead_id = lead["lead_id"]
    return {
        "_track_url": f"{base}/t/click/{lead_id}/{touch}?u={quote_plus(summit_url)}",
        "_unsub_url": f"{base}/t/unsub/{lead_id}",
        "_pixel_url": f"{base}/t/open/{lead_id}/{touch}.gif",
    }


def _wrap_html(text_body: str, pixel_url: str) -> str:
    """Convert plaintext to minimal HTML and inject open-tracking pixel."""
    html_lines = [
        f'<p>{line}</p>' if line.strip() else '<br/>'
        for line in text_body.split("\n")
    ]
    html = "\n".join(html_lines)
    if pixel_url:
        html += f'\n<img src="{pixel_url}" width="1" height="1" alt="" style="display:none"/>'
    return f"<html><body style='font-family:Arial,sans-serif;font-size:14px'>{html}</body></html>"


def _send_smtp(
    sender_email: str,
    sender_name: str,
    app_password: str,
    to_email: str,
    subject: str,
    text_body: str,
    html_body: str,
) -> None:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{sender_name} <{sender_email}>"
    msg["To"] = to_email
    msg["Reply-To"] = sender_email
    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(GMAIL_SMTP_HOST, GMAIL_SMTP_PORT, context=ctx) as server:
        server.login(sender_email, app_password)
        server.send_message(msg)


async def _send_one(
    lead: dict[str, Any],
    touch: int,
    cfg: dict[str, Any],
    storage: Any,
) -> bool:
    """Send a single email touch. Returns True on success."""
    if not lead.get("email"):
        return False
    if lead.get("unsubscribed"):
        return False

    # Inject tracking URLs into a copy of the lead before rendering
    tracked = {**lead, **_build_tracking_urls(lead, touch, cfg)}
    rendered = TEMPLATES[touch](tracked, cfg)
    html_body = _wrap_html(rendered["body_text"], tracked["_pixel_url"])

    sender_email = cfg["senderEmail"]
    sender_name = cfg.get("senderName", sender_email)
    app_password = cfg.get("gmailAppPassword", "")
    dry_run = bool(cfg.get("dryRun", True))

    if dry_run:
        Actor.log.info(
            f"[DRY RUN] Would send Touch-{touch} to {lead['email']} "
            f"({lead['company_name']}) — subj='{rendered['subject']}'"
        )
        await Actor.push_data({
            "lead_id": lead["lead_id"],
            "touch": touch,
            "preview_subject": rendered["subject"],
            "preview_body": rendered["body_text"],
            "to_email": lead["email"],
            "dry_run": True,
        })
        return True

    if not app_password:
        Actor.log.error("gmailAppPassword not provided and dryRun=false — cannot send.")
        return False

    try:
        # SMTP is blocking — run in thread
        await asyncio.to_thread(
            _send_smtp,
            sender_email,
            sender_name,
            app_password,
            lead["email"],
            rendered["subject"],
            rendered["body_text"],
            html_body,
        )
        now = datetime.now(timezone.utc).isoformat()
        update = {f"{['', 'first', 'second', 'third'][touch]}_email_sent_at": now,
                  f"touch_{touch}_subject_variant": rendered.get("variant_id"),
                  f"touch_{touch}_subject_used": rendered["subject"],
                  "engagement_status": f"touch_{touch}_sent",
                  "last_action_at": now}
        await storage.update_lead(lead["lead_id"], update)
        Actor.log.info(f"Sent Touch-{touch} to {lead['email']} ({lead['company_name']}) | variant={rendered.get('variant_id')}")
        return True
    except smtplib.SMTPException as e:
        Actor.log.error(f"SMTP error sending to {lead.get('email')}: {e}")
        return False
    except Exception as e:
        Actor.log.exception(f"Unexpected error sending to {lead.get('email')}: {e}")
        return False


async def _rate_limited_send(
    leads: list[dict[str, Any]],
    touch: int,
    cfg: dict[str, Any],
    storage: Any,
) -> int:
    """Send to all leads with a per-minute rate limit."""
    sent = 0
    delay = 60.0 / SEND_RATE_PER_MIN
    for lead in leads:
        ok = await _send_one(lead, touch, cfg, storage)
        if ok:
            sent += 1
        await asyncio.sleep(delay)
    return sent


async def send_touch_one(
    leads: list[dict[str, Any]],
    cfg: dict[str, Any],
    storage: Any,
) -> int:
    eligible = [
        l for l in leads
        if l.get("qualification_status") == "qualified"
        and l.get("email")
        and not l.get("first_email_sent_at")
        and not l.get("unsubscribed")
    ]
    Actor.log.info(f"Touch-1 eligible: {len(eligible)} of {len(leads)} leads")
    return await _rate_limited_send(eligible, 1, cfg, storage)


async def send_follow_ups(cfg: dict[str, Any], storage: Any) -> int:
    """Sends Touch-2 (day 4 after Touch-1) and Touch-3 (day 10 after Touch-1)."""
    now = datetime.now(timezone.utc)
    all_leads = await storage.fetch_leads(filters={})
    sent = 0

    # Touch 2: 4+ days since touch 1, no touch 2 yet, not replied/registered
    t2_due = []
    for l in all_leads:
        if l.get("replied") or l.get("registered_for_summit") or l.get("unsubscribed"):
            continue
        ts = l.get("first_email_sent_at")
        if not ts or l.get("second_email_sent_at"):
            continue
        try:
            t1 = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if now - t1 >= timedelta(days=4):
                t2_due.append(l)
        except ValueError:
            continue

    Actor.log.info(f"Touch-2 due: {len(t2_due)} leads")
    sent += await _rate_limited_send(t2_due, 2, cfg, storage)

    # Touch 3: 10+ days since touch 1, no touch 3 yet
    t3_due = []
    for l in all_leads:
        if l.get("replied") or l.get("registered_for_summit") or l.get("unsubscribed"):
            continue
        ts = l.get("first_email_sent_at")
        if not ts or l.get("third_email_sent_at"):
            continue
        try:
            t1 = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if now - t1 >= timedelta(days=10):
                t3_due.append(l)
        except ValueError:
            continue

    Actor.log.info(f"Touch-3 due: {len(t3_due)} leads")
    sent += await _rate_limited_send(t3_due, 3, cfg, storage)
    return sent
