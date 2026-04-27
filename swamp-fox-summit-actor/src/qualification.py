"""Qualification — score and tag each lead, assign a producer."""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from apify import Actor

# Swamp Fox producer routing per memory:
# Jeremy, Tyler, Chris Lands, Rick, Barbara Malcolm
PRODUCER_ROUTING = {
    "logging": "Tyler",
    "timber": "Chris Lands",
    "forestry": "Chris Lands",
    "trucking": "Jeremy",
}

# Loggers Technology Summit audience priority weighting.
# Logging/timber/forestry = core audience. Trucking only relevant when it's log hauling.
INDUSTRY_FIT_BOOST = {
    "logging": 20,
    "timber": 18,
    "forestry": 18,
    "trucking": 8,
}

# States Swamp Fox is licensed in (per user)
LICENSED_STATES = {"SC", "NC", "GA", "AL", "TN", "FL", "MS", "VA", "KY"}


def _make_lead_id(lead: dict[str, Any]) -> str:
    seed = f"{lead.get('company_name','')}|{lead.get('phone','')}|{lead.get('website','')}"
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]


def _score_lead(lead: dict[str, Any], min_signals: int) -> tuple[int, str, list[str]]:
    """Returns (score, status, reasons)."""
    score = 0
    reasons: list[str] = []

    if lead.get("website"):
        score += 15
        reasons.append("has_website")
    if lead.get("phone"):
        score += 10
        reasons.append("has_phone")
    if lead.get("email"):
        score += 25
        reasons.append("has_email")
    if (lead.get("review_count") or 0) >= 3:
        score += 10
        reasons.append("active_reviews")

    # Industry fit boost — logging/timber/forestry are core Summit audience
    industry = lead.get("industry_tag", "trucking")
    fit = INDUSTRY_FIT_BOOST.get(industry, 0)
    if fit:
        score += fit
        reasons.append(f"industry_fit_{industry}")

    # Decision-maker contact boost — LinkedIn matched a real person
    if lead.get("contact_name") and lead.get("contact_title"):
        title_score = lead.get("contact_role_tag")
        if title_score in ("owner", "vp_ops", "fleet_manager", "safety_lead"):
            score += 15
            reasons.append(f"decision_maker_{title_score}")
        else:
            score += 8
            reasons.append("contact_identified")

    # Email source quality
    if lead.get("email_source") == "pattern_guess":
        score -= 5  # less reliable than confirmed
        reasons.append("guessed_email")

    # Years in business signal — established operators are higher fit
    yib = lead.get("yp_years_in_business")
    if yib and yib >= 10:
        score += 8
        reasons.append("established_business")

    # BBB rating
    bbb = lead.get("bbb_rating", "")
    if bbb and bbb.startswith("A"):
        score += 5
        reasons.append("bbb_a_rated")

    # Geographic weighting — closer to Birmingham = higher Summit relevance
    dist = lead.get("distance_to_birmingham_km")
    if dist is not None:
        if dist <= 100:
            score += 25
            reasons.append("near_birmingham")
        elif dist <= 250:
            score += 15
            reasons.append("regional_birmingham")
        elif dist <= 500:
            score += 8
            reasons.append("drivable_birmingham")
        else:
            score += 2

    # Licensed-state filter
    state = (lead.get("state") or "").upper()
    if state and state not in LICENSED_STATES:
        return 0, "out_of_region", ["unlicensed_state"]

    # Determine status
    signal_count = sum(1 for k in ("website", "phone", "email") if lead.get(k))
    if signal_count + (1 if (lead.get("review_count") or 0) >= 3 else 0) < min_signals:
        return score, "insufficient_signals", reasons
    if not lead.get("email"):
        return score, "no_email", reasons
    return score, "qualified", reasons


def qualify_and_score(leads: list[dict[str, Any]], cfg: dict[str, Any]) -> list[dict[str, Any]]:
    min_signals = int(cfg.get("minFleetSignals", 2))
    now = datetime.now(timezone.utc).isoformat()

    qualified = []
    for lead in leads:
        score, status, reasons = _score_lead(lead, min_signals)
        producer = PRODUCER_ROUTING.get(lead.get("industry_tag", "trucking"), "Jeremy")

        record = {
            **lead,
            "lead_id": _make_lead_id(lead),
            "lead_score": score,
            "qualification_status": status,
            "qualification_reasons": reasons,
            "producer_assigned": producer,
            "scraped_at": now,
            "first_email_sent_at": None,
            "first_email_opened_at": None,
            "first_email_clicked_at": None,
            "second_email_sent_at": None,
            "second_email_opened_at": None,
            "third_email_sent_at": None,
            "registered_for_summit": False,
            "replied": False,
            "call_booked": False,
            "unsubscribed": False,
            "engagement_status": "new",
            "last_action_at": now,
        }
        qualified.append(record)

    qualified.sort(key=lambda r: r["lead_score"], reverse=True)
    Actor.log.info(
        f"Qualified breakdown: "
        + ", ".join(
            f"{s}={sum(1 for r in qualified if r['qualification_status']==s)}"
            for s in {r["qualification_status"] for r in qualified}
        )
    )
    return qualified
