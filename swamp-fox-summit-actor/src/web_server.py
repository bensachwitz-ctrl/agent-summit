"""Tracking web server — runs as part of the Actor process to serve:
  GET /t/open/<lead_id>/<touch>.gif    → record open + return 1x1 GIF
  GET /t/click/<lead_id>/<touch>?u=<url> → record click + redirect
  GET /t/unsub/<lead_id>                 → mark unsubscribed + return confirmation page
  GET /t/registered/<lead_id>            → mark registered (call after form submit)
  GET /health                            → liveness probe

Started by `run_tracking_server(storage)` and exposed via the Apify Web Server URL
which is automatically published when `webServerEnabled: true` in actor.json
or when running on the Apify platform with port 4321 exposed.
"""
from __future__ import annotations

import base64
from datetime import datetime, timezone
from urllib.parse import unquote

from aiohttp import web
from apify import Actor

PIXEL_GIF = base64.b64decode(
    "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"
)

UNSUB_PAGE_HTML = """<!doctype html>
<html><head><meta charset="utf-8"><title>Unsubscribed</title>
<style>body{font-family:Arial,sans-serif;max-width:600px;margin:80px auto;padding:0 20px;color:#333}
h1{color:#1a4d2e}p{line-height:1.5}</style></head>
<body><h1>You're unsubscribed.</h1>
<p>You won't receive further outreach from us about the Loggers Technology Summit.</p>
<p>If this was a mistake, just reply to any of our emails and we'll add you back.</p>
<p>— Ben Sachwitz, Swamp Fox Agency</p></body></html>"""


def build_app(storage) -> web.Application:
    app = web.Application()
    app["storage"] = storage
    app["cfg"] = getattr(storage, "cfg", {}) or {}

    async def health(request):
        return web.json_response({"status": "ok"})

    async def track_open(request):
        lead_id = request.match_info["lead_id"]
        touch = int(request.match_info["touch"])
        Actor.log.info(f"OPEN tracked — lead={lead_id} touch={touch}")
        try:
            field = {1: "first_email_opened_at", 2: "second_email_opened_at"}.get(touch)
            if field:
                kv = await storage.kv_store.get_value(f"LEAD_{lead_id}")
                if kv and not kv.get(field):
                    await storage.update_lead(lead_id, {
                        field: datetime.now(timezone.utc).isoformat(),
                        "engagement_status": f"touch_{touch}_opened",
                    })
        except Exception as e:
            Actor.log.warning(f"open tracking failed for {lead_id}: {e}")
        return web.Response(body=PIXEL_GIF, content_type="image/gif",
                            headers={"Cache-Control": "no-store, no-cache, must-revalidate"})

    async def track_click(request):
        lead_id = request.match_info["lead_id"]
        touch = int(request.match_info["touch"])
        target = unquote(request.query.get("u", "https://swampfoxagency.com/the-summit/"))
        Actor.log.info(f"CLICK tracked — lead={lead_id} touch={touch} target={target}")
        try:
            await storage.update_lead(lead_id, {
                "first_email_clicked_at" if touch == 1 else f"touch_{touch}_clicked_at":
                    datetime.now(timezone.utc).isoformat(),
                "engagement_status": f"touch_{touch}_clicked",
            })
            # Trigger engagement alert
            from .reply_alerts import send_engagement_alert
            updated = await storage.kv_store.get_value(f"LEAD_{lead_id}")
            if updated:
                await send_engagement_alert(updated, "clicked", app["cfg"])
        except Exception as e:
            Actor.log.warning(f"click tracking failed for {lead_id}: {e}")
        raise web.HTTPFound(location=target)

    async def track_unsub(request):
        lead_id = request.match_info["lead_id"]
        Actor.log.info(f"UNSUB tracked — lead={lead_id}")
        try:
            await storage.update_lead(lead_id, {
                "unsubscribed": True,
                "engagement_status": "unsubscribed",
                "last_action_at": datetime.now(timezone.utc).isoformat(),
            })
        except Exception as e:
            Actor.log.warning(f"unsub tracking failed for {lead_id}: {e}")
        return web.Response(text=UNSUB_PAGE_HTML, content_type="text/html")

    async def track_registered(request):
        lead_id = request.match_info["lead_id"]
        Actor.log.info(f"REGISTERED tracked — lead={lead_id}")
        try:
            await storage.update_lead(lead_id, {
                "registered_for_summit": True,
                "engagement_status": "registered",
                "last_action_at": datetime.now(timezone.utc).isoformat(),
            })
            # Trigger engagement alert
            from .reply_alerts import send_engagement_alert
            updated = await storage.kv_store.get_value(f"LEAD_{lead_id}")
            if updated:
                await send_engagement_alert(updated, "registered", app["cfg"])
        except Exception as e:
            Actor.log.warning(f"registration tracking failed for {lead_id}: {e}")
        return web.json_response({"status": "registered"})

    app.router.add_get("/health", health)
    app.router.add_get("/t/open/{lead_id}/{touch}.gif", track_open)
    app.router.add_get("/t/click/{lead_id}/{touch}", track_click)
    app.router.add_get("/t/unsub/{lead_id}", track_unsub)
    app.router.add_get("/t/registered/{lead_id}", track_registered)
    return app


async def run_tracking_server(storage, port: int = 4321) -> web.AppRunner:
    """Start the tracking server in the background — returns runner so caller can stop it."""
    app = build_app(storage)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    Actor.log.info(f"Tracking server listening on port {port}")
    return runner
