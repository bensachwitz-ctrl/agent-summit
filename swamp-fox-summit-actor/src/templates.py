"""Email templates for the Loggers Technology Summit (4th Annual) outreach.

Event: Aug 28-29, 2026 | Birmingham, AL
- Aug 28, 6PM CT: Welcome Reception @ Alabama Sports Hall of Fame + Pinnacle Award
- Aug 29: Full Summit @ Barber Motorsports Park

Design principles for high conversion:
  • Subject lines selected per-lead from variants in subject_lines.py
  • First sentence = specific value, not pleasantries
  • Hard-stop CTA placement (>>> Register here)
  • Role-specific second sentence for personalization
  • Short paragraphs (B2B execs scan, don't read)
  • One clear ask per email
  • No filler words, no "I hope this finds you well", no "just checking in"
"""
from __future__ import annotations

from typing import Any

from .subject_lines import select_subject


SWAMP_FOX_FOOTER = """
--
Ben Sachwitz
Swamp Fox Agency | Host — 4th Annual Loggers Technology Summit
Birmingham, AL | Aug 28–29, 2026

Register: {summit_url}
Hotel block: Westin Birmingham — 221 Richard Arrington Jr Blvd N

You received this because we identified your company as a {industry} operator in the Southeastern forestry corridor.
Reply STOP or unsubscribe: {unsub_url}
"""


# Industry-specific value propositions
INDUSTRY_HOOKS = {
    "logging": {
        "headline": "logging operations across the Southeast",
        "pain": "auto liability premium pressure, driver shortages, and the safety expectations carriers now require from log haulers",
        "value": "live telematics demos, dash cam and collision mitigation systems, and driver scorecard sessions built specifically for log truck fleets",
        "session": "Driver Safety + Fleet Management",
    },
    "timber": {
        "headline": "timber harvesting and wood products operators",
        "pain": "tightening insurance markets for timber operations and equipment exposure carriers are reluctant to write",
        "value": "GPS tracking, equipment monitoring tech, and risk management content built for timber harvesters",
        "session": "Equipment Innovation + Insurance & Risk",
    },
    "forestry": {
        "headline": "forestry contractors and fleet operators",
        "pain": "specialty forestry exposures most standard markets won't touch and the operational gaps that drive losses",
        "value": "loss prevention strategies, compliance content, and a vendor floor of forestry tech partners",
        "session": "Insurance & Risk + Technology in Logging",
    },
    "trucking": {
        "headline": "log haulers and forestry transportation fleets",
        "pain": "CSA score pressure, nuclear verdict trends, and the renewal cycle hitting log truck fleets hardest",
        "value": "fleet performance benchmarks and telematics content from carriers who understand log hauling",
        "session": "Fleet Management + Driver Safety",
    },
}


# Role-aware second sentence — adds when LinkedIn matched a decision maker
ROLE_INTROS = {
    "owner": "As the {title_lower} at {company}, you're the one weighing whether the time and cost makes sense, so I'll keep this direct.",
    "vp_ops": "Given your role on the operations side, the Day 2 Fleet Management content is built directly for what you handle.",
    "fleet_manager": "Your role is the exact target audience for the Day 2 sessions on telematics, driver scorecards, and fleet performance.",
    "safety_lead": "The Driver Safety track on Day 2 is built for safety leaders managing logging fleet exposures, which is your work directly.",
    "ops_manager": "The operations content on Day 2 covers telematics, fleet performance, and equipment innovation — the core of your role.",
    "gm": "As GM, the Day 1 Pinnacle Award reception is the right networking room for {company} — operators in your seat from across the Southeast.",
    "risk_manager": "The Insurance & Risk track on Day 2 covers loss prevention strategies specific to logging — directly your area.",
    "transport_manager": "Day 2 Fleet Management content covers the transportation side of forestry operations — built for your role.",
}


def _hook(industry: str) -> dict[str, str]:
    return INDUSTRY_HOOKS.get(industry, INDUSTRY_HOOKS["logging"])


def _greeting(lead: dict[str, Any]) -> str:
    first = lead.get("contact_first_name")
    if first:
        return f"Hi {first},"
    return f"Hi {lead.get('company_name', 'there')} team,"


def _role_intro(lead: dict[str, Any]) -> str:
    tag = lead.get("contact_role_tag")
    title = lead.get("contact_title", "")
    company = lead.get("company_name", "your team")
    if tag in ROLE_INTROS:
        return "\n\n" + ROLE_INTROS[tag].format(
            title_lower=title.lower() if title else "owner",
            company=company,
        )
    return ""


def render_touch_one(lead: dict[str, Any], cfg: dict[str, Any]) -> dict[str, str]:
    """First-touch invitation — registration form is the primary CTA."""
    industry = lead.get("industry_tag", "logging")
    hook = _hook(industry)
    company = lead.get("company_name", "your team")
    summit_url = cfg.get("summitUrl", "https://swampfoxagency.com/the-summit/")
    register_url = cfg.get("registrationFormUrl") or f"{summit_url}#register"
    unsub_url = lead.get("_unsub_url", f"{summit_url}#unsubscribe")
    track_url = lead.get("_track_url", register_url)

    subject, variant_id = select_subject(lead, 1)

    body = f"""{_greeting(lead)}

I run insurance and risk for {hook['headline']} at Swamp Fox Agency. We're hosting the 4th Annual Loggers Technology Summit in Birmingham, August 28–29, and I'd like to extend a personal invitation to {company}.{_role_intro(lead)}

This event is built specifically for logging owners, forestry fleet operators, safety leaders, and the technology partners serving them. The networking is the highest-value piece — operators dealing with the same renewal pressures, driver issues, and equipment decisions you are, in the same corridor.

Day 2 content focuses on the {hook['session']} tracks, with {hook['value']}.

Schedule:
  • Day 1 (Aug 28, 6 PM CT) — Welcome Reception at Alabama Sports Hall of Fame, including the Pinnacle Award recognition for excellence in the logging industry
  • Day 2 (Aug 29) — Full Summit at Barber Motorsports Park, where the outdoor venue allows live equipment demos and heavy-truck safety tech you cannot see at a hotel conference

>>> Register here (60 seconds, free for qualified operators):
{track_url}

Once registered, you will receive the full agenda and access to the Westin Birmingham hotel block.

If a brief call before the event makes more sense, reply with a time and I will set it up.

{SWAMP_FOX_FOOTER.format(summit_url=summit_url, industry=industry, unsub_url=unsub_url)}"""

    return {"subject": subject, "body_text": body, "variant_id": variant_id}


def render_touch_two(lead: dict[str, Any], cfg: dict[str, Any]) -> dict[str, str]:
    """Day-4 follow-up — Pinnacle Award + reception networking + offer call."""
    industry = lead.get("industry_tag", "logging")
    hook = _hook(industry)
    company = lead.get("company_name", "your team")
    summit_url = cfg.get("summitUrl", "https://swampfoxagency.com/the-summit/")
    register_url = cfg.get("registrationFormUrl") or f"{summit_url}#register"
    calendly = cfg.get("calendlyUrl") or ""
    unsub_url = lead.get("_unsub_url", f"{summit_url}#unsubscribe")
    track_url = lead.get("_track_url", register_url)

    subject, variant_id = select_subject(lead, 2)

    cta = (
        f"Schedule 15 minutes here: {calendly}"
        if calendly
        else "Reply with a time and I will send a calendar invite."
    )

    body = f"""{_greeting(lead)}

Following up on the Loggers Technology Summit. Three points worth surfacing:

1. The Welcome Reception is the real networking room.
August 28 at the Alabama Sports Hall of Fame — carriers, agents, vendors, and logging leaders all in one room before the formal sessions begin. The Pinnacle Award is presented that evening, recognizing operators setting the standard on safety, operations, and innovation.

2. The Day 2 technical content.
Sessions at Barber Motorsports Park are built around {hook['pain']}. The {hook['session']} tracks alone justify the trip — telematics platforms, dash cam and collision mitigation systems, and live equipment demos in an outdoor setting.

3. Hotel logistics.
The Westin Birmingham is the official block (221 Richard Arrington Jr Blvd N). Rooms are filling.

>>> Registration form (60 seconds, free for qualified operators):
{track_url}

If you would rather discuss {hook['pain']} for {company} specifically, I am happy to schedule a brief call before the event. {cta}

{SWAMP_FOX_FOOTER.format(summit_url=summit_url, industry=industry, unsub_url=unsub_url)}"""

    return {"subject": subject, "body_text": body, "variant_id": variant_id}


def render_touch_three(lead: dict[str, Any], cfg: dict[str, Any]) -> dict[str, str]:
    """Day-10 final touch — registration deadline + soft fallback."""
    industry = lead.get("industry_tag", "logging")
    company = lead.get("company_name", "your team")
    summit_url = cfg.get("summitUrl", "https://swampfoxagency.com/the-summit/")
    register_url = cfg.get("registrationFormUrl") or f"{summit_url}#register"
    unsub_url = lead.get("_unsub_url", f"{summit_url}#unsubscribe")
    track_url = lead.get("_track_url", register_url)

    subject, variant_id = select_subject(lead, 3)

    body = f"""{_greeting(lead)}

Final note from me on this one. We are closing Loggers Technology Summit registration ahead of the event so we can finalize headcount with the Westin and Barber Motorsports Park.

If {company} can send anyone — owner, fleet manager, safety lead, or operations — the registration form is here:

>>> {track_url}

What is different about this Summit:
  • Day 1 reception at Alabama Sports Hall of Fame: networking room of logging operators, carriers, and vendors from across the Southeast
  • Day 2 at Barber Motorsports Park: outdoor venue allowing real equipment demos rather than slide presentations
  • Pinnacle Award recognition for industry leaders on Day 1

If timing is not right this year, reply "next year" and I will reach out when registration opens for the 5th annual.

If you would rather have a conversation about your renewal cycle whenever it comes up, reply to this email and I will keep your contact in our follow-up list. No further automated emails after this one.

{SWAMP_FOX_FOOTER.format(summit_url=summit_url, industry=industry, unsub_url=unsub_url)}"""

    return {"subject": subject, "body_text": body, "variant_id": variant_id}


TEMPLATES = {
    1: render_touch_one,
    2: render_touch_two,
    3: render_touch_three,
}
