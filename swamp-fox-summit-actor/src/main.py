"""Main orchestrator for the Loggers Technology Summit Outreach Actor.

Pipeline (source modes):
  1. Google Maps  → raw business listings
  2. LinkedIn     → company pages + decision-maker employees
  3. Multi-source → Yellow Pages, BBB, deep contact scraper
  4. Website      → email extraction from contact pages
  5. Email finder → pattern-based generation when name+domain known
  6. Qualify      → score, route to producer, set status

Outreach modes:
  source_only            → run pipeline, write storage, no emails
  source_and_outreach    → above + Touch-1 to qualified leads
  outreach_only          → Touch-1 to existing dataset
  follow_up_only         → Touch-2 (day 4) + Touch-3 (day 10)
  metrics_sync           → reply check + storage refresh

A web server (aiohttp) starts in every mode that touches sending. It serves
tracking pixels, click redirects, unsubscribe pages, and a registration
confirmation endpoint.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from apify import Actor

from .lead_sourcing import source_leads
from .linkedin import enrich_with_linkedin
from .multi_source import enrich_multi_source
from .enrichment import enrich_leads
from .email_finder import find_emails
from .qualification import qualify_and_score
from .outreach import send_touch_one, send_follow_ups
from .storage import StorageRouter
from .tracking import sync_tracking_events
from .reply_monitor import check_for_replies
from .web_server import run_tracking_server

log = logging.getLogger(__name__)


async def main() -> None:
    async with Actor:
        cfg = await Actor.get_input() or {}
        mode = cfg.get("mode", "source_only")
        Actor.log.info(f"=== Loggers Technology Summit Outreach Agent | mode={mode} ===")

        storage = StorageRouter(cfg)
        await storage.initialize()

        # Resolve tracking base URL — use Apify Web Server URL if not provided
        if not cfg.get("trackingBaseUrl"):
            try:
                cfg["trackingBaseUrl"] = Actor.config.standby_url or ""
            except AttributeError:
                cfg["trackingBaseUrl"] = ""
        if cfg.get("trackingBaseUrl"):
            Actor.log.info(f"Tracking base URL: {cfg['trackingBaseUrl']}")

        # Start web server for tracking endpoints (only when sending or syncing)
        web_runner = None
        if mode in ("source_and_outreach", "outreach_only", "follow_up_only", "metrics_sync"):
            try:
                # Make cfg accessible to web server for engagement alerts
                storage.cfg = cfg
                web_runner = await run_tracking_server(storage, port=4321)
            except Exception as e:
                Actor.log.warning(f"Tracking server failed to start: {e}")

        run_started = datetime.now(timezone.utc).isoformat()
        stats = {"sourced": 0, "qualified": 0, "emailed": 0, "replies": 0, "errors": 0}

        try:
            if mode in ("source_only", "source_and_outreach"):
                # ── Phase 1: Google Maps ─────────────────────────────────────
                Actor.log.info("Phase 1 — Sourcing leads from Google Maps")
                raw_leads = await source_leads(cfg)
                stats["sourced"] = len(raw_leads)
                if not raw_leads:
                    Actor.log.warning("No leads sourced — exiting pipeline.")
                else:
                    # ── Phase 2: LinkedIn (company + decision maker) ─────────
                    Actor.log.info("Phase 2 — LinkedIn enrichment")
                    raw_leads = await enrich_with_linkedin(raw_leads, cfg)

                    # ── Phase 3: Multi-source (YP, BBB, contact scraper) ─────
                    Actor.log.info("Phase 3 — Multi-source enrichment")
                    raw_leads = await enrich_multi_source(raw_leads, cfg)

                    # ── Phase 4: Website crawl for emails ────────────────────
                    Actor.log.info("Phase 4 — Website email enrichment")
                    raw_leads = await enrich_leads(raw_leads, cfg)

                    # ── Phase 5: Email pattern finder ────────────────────────
                    Actor.log.info("Phase 5 — Email pattern generation")
                    raw_leads = await find_emails(raw_leads, cfg)

                    # ── Phase 6: Qualify ──────────────────────────────────────
                    Actor.log.info("Phase 6 — Qualification + scoring")
                    qualified = qualify_and_score(raw_leads, cfg)
                    stats["qualified"] = sum(
                        1 for l in qualified if l["qualification_status"] == "qualified"
                    )

                    # ── Phase 7: Build intelligence briefs ────────────────────
                    Actor.log.info("Phase 7 — Building lead intelligence briefs")
                    from .lead_intel import add_intel_briefs
                    qualified = add_intel_briefs(qualified)

                    Actor.log.info(f"Phase 8 — Writing {len(qualified)} leads to storage")
                    await storage.upsert_leads(qualified)

                    if mode == "source_and_outreach":
                        Actor.log.info("Phase 9 — Sending Touch-1 emails")
                        sent = await send_touch_one(qualified, cfg, storage)
                        stats["emailed"] = sent

            elif mode == "outreach_only":
                leads = await storage.fetch_leads(filters={"engagement_status": "new"})
                Actor.log.info(f"Sending Touch-1 to {len(leads)} eligible leads")
                stats["emailed"] = await send_touch_one(leads, cfg, storage)

            elif mode == "follow_up_only":
                Actor.log.info("Checking replies before follow-ups (auto-suppression)")
                stats["replies"] = await check_for_replies(cfg, storage)
                Actor.log.info("Sending follow-up sequence (Touch-2 / Touch-3)")
                stats["emailed"] = await send_follow_ups(cfg, storage)

            elif mode == "metrics_sync":
                Actor.log.info("Checking replies in inbox")
                stats["replies"] = await check_for_replies(cfg, storage)
                Actor.log.info("Syncing engagement metrics across storage layers")
                metric_data = await sync_tracking_events(cfg, storage)
                stats.update(metric_data)

            else:
                raise ValueError(f"Unknown mode: {mode}")

            await Actor.set_value(
                "RUN_SUMMARY",
                {
                    "run_started": run_started,
                    "run_completed": datetime.now(timezone.utc).isoformat(),
                    "mode": mode,
                    "stats": stats,
                    "dry_run": cfg.get("dryRun", True),
                    "tracking_base_url": cfg.get("trackingBaseUrl"),
                },
            )
            Actor.log.info(f"=== Run complete | {stats} ===")

        except Exception as e:
            Actor.log.exception(f"Run failed: {e}")
            stats["errors"] += 1
            raise
        finally:
            if web_runner:
                # Keep the server alive for a few minutes after sends to catch immediate opens/clicks
                if mode in ("source_and_outreach", "outreach_only", "follow_up_only"):
                    keep_alive_minutes = int(cfg.get("trackingKeepAliveMinutes", 10))
                    Actor.log.info(f"Keeping tracking server alive {keep_alive_minutes} min for opens/clicks")
                    await asyncio.sleep(keep_alive_minutes * 60)
                await web_runner.cleanup()
