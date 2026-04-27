"""Tracking — serves open-pixel and click-redirect endpoints via Apify Web Server.

The Web Server URL is exposed as `Actor.config.web_server_url`. Each Actor
run gets a unique public URL while running.

Endpoints:
  GET /t/open/<lead_id>/<touch>.gif   → record open, return 1x1 GIF
  GET /t/click/<lead_id>/<touch>      → record click, redirect to ?u=<encoded>
  GET /t/unsub/<lead_id>              → mark unsubscribed
"""
from __future__ import annotations

import base64
from datetime import datetime, timezone
from typing import Any

from apify import Actor

# 1x1 transparent GIF
PIXEL_GIF = base64.b64decode(
    "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"
)


def record_open(storage: Any, lead_id: str, touch: int) -> None:
    """Synchronous-friendly hook called from the web handler."""
    field = {1: "first_email_opened_at", 2: "second_email_opened_at"}.get(touch)
    if not field:
        return
    now = datetime.now(timezone.utc).isoformat()

    async def _do():
        existing = await storage.kv_store.get_value(f"LEAD_{lead_id}")
        if existing and not existing.get(field):
            await storage.update_lead(lead_id, {field: now, "engagement_status": f"touch_{touch}_opened"})

    import asyncio
    asyncio.create_task(_do())


def record_click(storage: Any, lead_id: str, touch: int) -> None:
    field = "first_email_clicked_at" if touch == 1 else f"touch_{touch}_clicked_at"
    now = datetime.now(timezone.utc).isoformat()

    async def _do():
        existing = await storage.kv_store.get_value(f"LEAD_{lead_id}")
        if existing:
            updates = {field: now, "engagement_status": f"touch_{touch}_clicked"}
            # If they clicked the registration link, mark them as registered (best-effort)
            updates["registered_for_summit"] = True
            await storage.update_lead(lead_id, updates)

    import asyncio
    asyncio.create_task(_do())


def record_unsubscribe(storage: Any, lead_id: str) -> None:
    now = datetime.now(timezone.utc).isoformat()

    async def _do():
        await storage.update_lead(lead_id, {
            "unsubscribed": True,
            "engagement_status": "unsubscribed",
            "last_action_at": now,
        })

    import asyncio
    asyncio.create_task(_do())


async def sync_tracking_events(cfg: dict[str, Any], storage: Any) -> dict[str, int]:
    """Pull aggregate metrics from current state and re-emit to all storage layers.

    Useful for `metrics_sync` mode — it forces Sheets and Fabric to reflect
    the latest engagement state without triggering new sends.
    """
    leads = await storage.fetch_leads(filters={})
    if not leads:
        return {"synced": 0}

    # Resync all to Sheets + Fabric
    await storage._upsert_to_sheets(leads)
    await storage._upsert_to_fabric(leads)

    metrics = {
        "total_leads": len(leads),
        "qualified": sum(1 for l in leads if l.get("qualification_status") == "qualified"),
        "touch_1_sent": sum(1 for l in leads if l.get("first_email_sent_at")),
        "touch_1_opened": sum(1 for l in leads if l.get("first_email_opened_at")),
        "touch_1_clicked": sum(1 for l in leads if l.get("first_email_clicked_at")),
        "touch_2_sent": sum(1 for l in leads if l.get("second_email_sent_at")),
        "touch_3_sent": sum(1 for l in leads if l.get("third_email_sent_at")),
        "registered": sum(1 for l in leads if l.get("registered_for_summit")),
        "replied": sum(1 for l in leads if l.get("replied")),
        "unsubscribed": sum(1 for l in leads if l.get("unsubscribed")),
    }
    Actor.log.info(f"Metrics sync complete: {metrics}")
    await Actor.set_value("ENGAGEMENT_METRICS", metrics)
    return metrics
