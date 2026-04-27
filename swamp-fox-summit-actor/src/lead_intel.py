"""Lead intelligence brief generator.

For each qualified lead, builds a sales-ready profile pulling from every
enrichment source. The brief is:
  - Stored on the lead record (`intel_brief` field)
  - Visible in Google Sheets and Fabric
  - Attached to reply alert emails so Ben has full context when leads respond
  - Used by templates for tailored conversation hooks

The brief is template-based (deterministic) with optional Claude API
enrichment for premium briefs (controlled by `enableAIBrief` config).
"""
from __future__ import annotations

from typing import Any


# Talking points per role — what to bring up when speaking with this person
ROLE_TALKING_POINTS = {
    "owner": [
        "Bottom-line cost of accident severity in logging — nuclear verdict trends",
        "Insurance market direction for 2026 — capacity and pricing outlook",
        "Pinnacle Award eligibility — recognizes operational excellence",
        "Whether their current carrier actually understands log-hauling exposure",
    ],
    "vp_ops": [
        "Telematics ROI data — which platforms log fleets are actually getting value from",
        "Driver retention strategies in tight forestry labor market",
        "Equipment uptime + maintenance scheduling tech",
    ],
    "fleet_manager": [
        "Real driver scorecard examples from comparable logging fleets",
        "Dash cam + collision mitigation systems — vendor matrix on Day 2",
        "CSA score management for log-hauling operations",
        "Fleet maintenance + DOT compliance automation",
    ],
    "safety_lead": [
        "Loss control programs that drive premium reductions for logging",
        "OSHA + DOT compliance specific to forestry transportation",
        "Driver training and onboarding best practices",
        "Incident reporting and root-cause analysis tools",
    ],
    "ops_manager": [
        "Operational efficiency tech — GPS tracking, dispatch optimization",
        "Equipment monitoring and predictive maintenance",
        "Workflow automation for forestry operations",
    ],
    "gm": [
        "Strategic operational benchmarks against peer logging companies",
        "Leadership development in family-owned logging businesses",
        "Succession planning and risk transfer strategies",
    ],
    "risk_manager": [
        "Carrier appetite analysis for forestry segment",
        "Loss control program ROI benchmarks",
        "Risk transfer alternatives — captive insurance for logging",
    ],
    "transport_manager": [
        "Hours-of-service compliance technology",
        "Route optimization for log hauling",
        "Driver safety scoring and coaching tools",
    ],
}


# Industry-specific conversation hooks
INDUSTRY_HOOKS = {
    "logging": [
        "Equipment exposure on logging sites (skidders, feller bunchers, loaders)",
        "Workers' comp rates in logging — among the highest in commercial insurance",
        "Premium pressure from auto liability claims involving log trucks",
    ],
    "timber": [
        "Tightening insurance markets for timber harvesting operations",
        "Property + equipment coverage gaps common in timber operations",
        "Specialty markets that price timber business fairly",
    ],
    "forestry": [
        "Niche carriers that write forestry contractors competitively",
        "Bundling strategies for forestry + transportation fleets",
        "Loss prevention specific to forestry exposures",
    ],
    "trucking": [
        "Auto liability premium trends for log haulers",
        "CSA score management and impact on insurance pricing",
        "Driver shortage and retention strategies",
    ],
}


def _format_section(title: str, lines: list[str]) -> str:
    if not lines:
        return ""
    body = "\n".join(f"  • {line}" for line in lines)
    return f"\n{title}\n{body}\n"


def _operating_context(lead: dict[str, Any]) -> list[str]:
    lines = []
    if lead.get("city") and lead.get("state"):
        lines.append(f"Based in {lead['city']}, {lead['state']}")
    dist = lead.get("distance_to_birmingham_km")
    if dist is not None:
        if dist <= 100:
            lines.append(f"~{int(dist)} km from Birmingham — local market, very high travel feasibility")
        elif dist <= 250:
            lines.append(f"~{int(dist)} km from Birmingham — regional, drivable in a day")
        elif dist <= 500:
            lines.append(f"~{int(dist)} km from Birmingham — long drive but feasible")
        else:
            lines.append(f"~{int(dist)} km from Birmingham — likely flying in")
    yib = lead.get("yp_years_in_business")
    if yib:
        lines.append(f"{yib} years in business")
    emp = lead.get("linkedin_employee_count")
    if emp:
        lines.append(f"~{emp} employees on LinkedIn")
    bbb = lead.get("bbb_rating")
    if bbb:
        accred = " (BBB Accredited)" if lead.get("bbb_accredited") else ""
        lines.append(f"BBB Rating: {bbb}{accred}")
    return lines


def _decision_maker_section(lead: dict[str, Any]) -> list[str]:
    lines = []
    name = lead.get("contact_name")
    title = lead.get("contact_title")
    if name:
        line = name
        if title:
            line += f" — {title}"
        lines.append(line)
    if lead.get("contact_linkedin_url"):
        lines.append(f"LinkedIn: {lead['contact_linkedin_url']}")
    return lines


def _signal_quality(lead: dict[str, Any]) -> list[str]:
    lines = []
    email_source = lead.get("email_source")
    if lead.get("email"):
        if email_source == "pattern_guess":
            lines.append(f"Email: {lead['email']} — generated pattern, NOT confirmed")
        else:
            lines.append(f"Email: {lead['email']} — confirmed from website/scraper")
    if lead.get("phone"):
        lines.append(f"Phone: {lead['phone']}")
    if lead.get("website"):
        lines.append(f"Website: {lead['website']}")
    return lines


def build_intel_brief(lead: dict[str, Any]) -> str:
    """Build the human-readable intelligence brief for this lead."""
    industry = lead.get("industry_tag", "logging")
    role_tag = lead.get("contact_role_tag")
    company = lead.get("company_name", "Unknown company")

    sections = []
    sections.append(f"=== LEAD INTELLIGENCE BRIEF: {company} ===")
    sections.append(f"Producer: {lead.get('producer_assigned', 'unassigned')} | Score: {lead.get('lead_score', 0)} | Industry: {industry}")

    sections.append(_format_section("OPERATING CONTEXT", _operating_context(lead)))
    sections.append(_format_section("DECISION MAKER", _decision_maker_section(lead)))
    sections.append(_format_section("CONTACT SIGNALS", _signal_quality(lead)))

    if role_tag and role_tag in ROLE_TALKING_POINTS:
        sections.append(_format_section(
            f"TALKING POINTS FOR {role_tag.upper().replace('_', ' ')}",
            ROLE_TALKING_POINTS[role_tag],
        ))

    sections.append(_format_section(
        f"INDUSTRY HOOKS ({industry.upper()})",
        INDUSTRY_HOOKS.get(industry, []),
    ))

    # Engagement context if any
    engagement = []
    if lead.get("first_email_sent_at"):
        engagement.append(f"Touch 1 sent: {lead['first_email_sent_at']}")
    if lead.get("first_email_opened_at"):
        engagement.append(f"Touch 1 opened: {lead['first_email_opened_at']}")
    if lead.get("first_email_clicked_at"):
        engagement.append(f"Touch 1 clicked registration link: {lead['first_email_clicked_at']}")
    if lead.get("registered_for_summit"):
        engagement.append("REGISTERED FOR SUMMIT")
    if engagement:
        sections.append(_format_section("ENGAGEMENT HISTORY", engagement))

    # Suggested next action
    next_action = _suggest_next_action(lead)
    if next_action:
        sections.append(f"\nSUGGESTED NEXT ACTION:\n  {next_action}\n")

    return "\n".join(sections)


def _suggest_next_action(lead: dict[str, Any]) -> str:
    """Recommend the next move based on engagement state."""
    if lead.get("registered_for_summit"):
        return "Personal welcome — they registered. Send hotel info + ask if they want to schedule a 1:1 at the event."
    if lead.get("replied"):
        return "Personal reply within 4 hours — they engaged directly. Reference their company specifics."
    if lead.get("first_email_clicked_at") and not lead.get("registered_for_summit"):
        return "They clicked but didn't register. Send a personal note — likely a question about logistics."
    if lead.get("first_email_opened_at") and not lead.get("first_email_clicked_at"):
        return "Opened but no click. Touch 2 will land — wait for follow-up cycle."
    if lead.get("first_email_sent_at") and not lead.get("first_email_opened_at"):
        return "No open yet. Either filtered or not seen — Touch 2 will retry."
    return "Cold lead — Touch 1 not sent yet."


def add_intel_briefs(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Mutate leads in place — adds intel_brief field."""
    for lead in leads:
        lead["intel_brief"] = build_intel_brief(lead)
    return leads
