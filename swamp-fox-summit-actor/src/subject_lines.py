"""Subject line variants — picked per-lead based on attributes for max open rate.

Selection logic prioritizes:
  1. Personalization (first name beats company-only)
  2. Local relevance (city/state mention beats generic)
  3. Specificity (named role or action beats abstract)
  4. Curiosity/question beats statement (when first name unknown)

A/B variant tracking: selected variant is stored on the lead record so we can
measure open rate per variant in metrics_sync.
"""
from __future__ import annotations

import hashlib
from typing import Any


# Touch 1 subject variants — ranked by typical B2B open rate for cold outreach
TOUCH_1_VARIANTS = [
    {
        "id": "t1_personal_question",
        "needs": ["contact_first_name"],
        "template": "{first_name}, sending anyone to the Loggers Summit Aug 28–29?",
        "weight": 10,
    },
    {
        "id": "t1_local_peers",
        "needs": ["state"],
        "template": "{state} logging operators heading to Birmingham Aug 28–29",
        "weight": 8,
    },
    {
        "id": "t1_named_invite",
        "needs": [],
        "template": "Invite for {company}: 4th Annual Loggers Technology Summit",
        "weight": 7,
    },
    {
        "id": "t1_role_hook",
        "needs": ["contact_role_tag"],
        "template": "Day 2 fleet content at the Loggers Summit — {company}",
        "weight": 8,
    },
    {
        "id": "t1_city_hook",
        "needs": ["city"],
        "template": "{city} → Birmingham, Aug 28–29 — Loggers Summit invite",
        "weight": 7,
    },
]

# Touch 2 — follow-up with curiosity + Pinnacle Award angle
TOUCH_2_VARIANTS = [
    {
        "id": "t2_pinnacle_personal",
        "needs": ["contact_first_name"],
        "template": "{first_name} — Pinnacle Award + Day 1 reception details",
        "weight": 10,
    },
    {
        "id": "t2_following_up",
        "needs": [],
        "template": "Following up on the Loggers Summit — quick note for {company}",
        "weight": 8,
    },
    {
        "id": "t2_reception_focus",
        "needs": [],
        "template": "Day 1 reception is the room you want — Loggers Summit",
        "weight": 7,
    },
    {
        "id": "t2_question",
        "needs": ["contact_first_name"],
        "template": "{first_name}, did the Loggers Summit invite get to you?",
        "weight": 9,
    },
]

# Touch 3 — final close, urgency without being spammy
TOUCH_3_VARIANTS = [
    {
        "id": "t3_closing_personal",
        "needs": ["contact_first_name"],
        "template": "{first_name} — closing Loggers Summit registration",
        "weight": 10,
    },
    {
        "id": "t3_headcount",
        "needs": [],
        "template": "Headcount cutoff: Loggers Summit Aug 28–29",
        "weight": 8,
    },
    {
        "id": "t3_final_note",
        "needs": [],
        "template": "Final note on the Aug 28–29 Summit — {company}",
        "weight": 7,
    },
]


VARIANTS_BY_TOUCH = {
    1: TOUCH_1_VARIANTS,
    2: TOUCH_2_VARIANTS,
    3: TOUCH_3_VARIANTS,
}


def _has_required(lead: dict[str, Any], needs: list[str]) -> bool:
    return all(lead.get(n) for n in needs)


def _hash_pick(lead_id: str, n: int) -> int:
    """Deterministic pick — same lead always gets same variant for consistency."""
    h = int(hashlib.md5(lead_id.encode()).hexdigest(), 16)
    return h % n


def select_subject(lead: dict[str, Any], touch: int) -> tuple[str, str]:
    """Returns (subject_text, variant_id) for the given lead and touch."""
    variants = VARIANTS_BY_TOUCH.get(touch, [])
    eligible = [v for v in variants if _has_required(lead, v["needs"])]
    if not eligible:
        eligible = [v for v in variants if not v["needs"]]

    # Weighted deterministic pick
    weights = [v["weight"] for v in eligible]
    total = sum(weights)
    pick_idx = _hash_pick(lead.get("lead_id", "default"), total)
    cumulative = 0
    chosen = eligible[0]
    for v, w in zip(eligible, weights):
        cumulative += w
        if pick_idx < cumulative:
            chosen = v
            break

    formatted = chosen["template"].format(
        first_name=lead.get("contact_first_name", ""),
        company=lead.get("company_name", "your company"),
        city=lead.get("city", ""),
        state=lead.get("state", ""),
    )
    return formatted, chosen["id"]
