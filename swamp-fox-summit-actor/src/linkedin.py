"""LinkedIn enrichment — pulls company pages and decision-maker employees.

Strategy:
  1. For each lead with a company name, search LinkedIn for the company page
     using `harvestapi/linkedin-company` (no login required for public data).
  2. From the company URL, pull employees filtered by relevant titles using
     `harvestapi/linkedin-company-employees`.
  3. Score employees by title fit and merge top decision maker into the lead.

Decision maker priority for logging/forestry operations:
  Owner / President / CEO > VP Operations > Fleet Manager > Safety Director
  > Operations Manager > General Manager > Risk Manager
"""
from __future__ import annotations

import re
from typing import Any

from apify import Actor

LINKEDIN_COMPANY_ACTOR = "harvestapi/linkedin-company"
LINKEDIN_EMPLOYEES_ACTOR = "harvestapi/linkedin-company-employees"

# Title scoring — higher = better fit for Summit invite (decision authority)
TITLE_SCORES: list[tuple[re.Pattern, int, str]] = [
    (re.compile(r"\b(owner|founder|president|ceo|principal)\b", re.I), 100, "owner"),
    (re.compile(r"\b(vp|vice president).*\b(operation|fleet|safety|risk)", re.I), 85, "vp_ops"),
    (re.compile(r"\bfleet\s*(manager|director|supervisor)\b", re.I), 80, "fleet_manager"),
    (re.compile(r"\bsafety\s*(manager|director|coordinator|officer)\b", re.I), 75, "safety_lead"),
    (re.compile(r"\b(operations|ops)\s*(manager|director|coordinator)\b", re.I), 70, "ops_manager"),
    (re.compile(r"\bgeneral\s*manager\b", re.I), 65, "gm"),
    (re.compile(r"\brisk\s*manager\b", re.I), 60, "risk_manager"),
    (re.compile(r"\b(transport|transportation|logistics)\s*manager\b", re.I), 55, "transport_manager"),
    (re.compile(r"\b(controller|cfo|finance director)\b", re.I), 40, "finance"),
]


def _score_title(title: str) -> tuple[int, str]:
    """Return (score, role_tag) for a job title."""
    if not title:
        return 0, "unknown"
    for pattern, score, tag in TITLE_SCORES:
        if pattern.search(title):
            return score, tag
    return 0, "other"


async def _find_linkedin_company(company_name: str, location: str | None) -> dict[str, Any] | None:
    """Search LinkedIn for a company page, return the best match."""
    try:
        run_input = {
            "queries": [f"{company_name} {location}" if location else company_name],
            "maxItems": 3,
        }
        run = await Actor.call(LINKEDIN_COMPANY_ACTOR, run_input=run_input, timeout_secs=120)
        if not run:
            return None
        items = (await Actor.apify_client.dataset(run["defaultDatasetId"]).list_items()).items
        if not items:
            return None
        # Pick best match — prefer name token overlap
        company_tokens = set(company_name.lower().split())
        best = None
        best_score = -1
        for item in items:
            name = (item.get("name") or "").lower()
            score = sum(1 for t in company_tokens if t in name)
            if score > best_score:
                best_score = score
                best = item
        return best
    except Exception as e:
        Actor.log.debug(f"LinkedIn company search failed for '{company_name}': {e}")
        return None


async def _scrape_employees(company_url: str, max_employees: int = 25) -> list[dict[str, Any]]:
    """Pull employees from a LinkedIn company page."""
    try:
        run_input = {
            "companyUrls": [company_url],
            "maxItems": max_employees,
            # Filter for relevant titles to reduce cost
            "currentJobTitles": [
                "owner", "president", "ceo", "vice president", "vp",
                "fleet manager", "safety director", "operations manager",
                "general manager", "risk manager", "transport manager"
            ],
        }
        run = await Actor.call(LINKEDIN_EMPLOYEES_ACTOR, run_input=run_input, timeout_secs=180)
        if not run:
            return []
        items = (await Actor.apify_client.dataset(run["defaultDatasetId"]).list_items()).items
        return items or []
    except Exception as e:
        Actor.log.debug(f"LinkedIn employees scrape failed for {company_url}: {e}")
        return []


def _pick_best_decision_maker(employees: list[dict[str, Any]]) -> dict[str, Any] | None:
    """From a list of employees, return the one with the highest title score."""
    scored = []
    for emp in employees:
        title = emp.get("currentJobTitle") or emp.get("headline") or ""
        score, tag = _score_title(title)
        if score > 0:
            scored.append((score, tag, emp))
    if not scored:
        return None
    scored.sort(key=lambda x: x[0], reverse=True)
    score, tag, best = scored[0]
    return {
        "name": best.get("name") or f"{best.get('firstName','')} {best.get('lastName','')}".strip(),
        "first_name": best.get("firstName"),
        "last_name": best.get("lastName"),
        "title": best.get("currentJobTitle") or best.get("headline"),
        "linkedin_url": best.get("profileUrl") or best.get("publicIdentifier"),
        "title_score": score,
        "role_tag": tag,
    }


async def enrich_with_linkedin(
    leads: list[dict[str, Any]],
    cfg: dict[str, Any],
) -> list[dict[str, Any]]:
    """Mutate leads in place — add LinkedIn company URL + best decision maker."""
    enabled = cfg.get("enableLinkedIn", True)
    if not enabled:
        Actor.log.info("LinkedIn enrichment disabled.")
        return leads

    max_to_enrich = int(cfg.get("linkedInMaxLeads", 100))
    # Only enrich top-scoring leads to control cost
    candidates = [
        l for l in leads
        if not l.get("contact_name") and l.get("company_name")
    ][:max_to_enrich]
    Actor.log.info(f"LinkedIn enriching {len(candidates)} leads (cap={max_to_enrich})")

    enriched_count = 0
    for lead in candidates:
        location = f"{lead.get('city','')} {lead.get('state','')}".strip()
        company = await _find_linkedin_company(lead["company_name"], location)
        if not company:
            continue

        lead["linkedin_company_url"] = company.get("url") or company.get("linkedinUrl")
        lead["linkedin_company_name"] = company.get("name")
        lead["linkedin_employee_count"] = company.get("employeeCount") or company.get("staffCount")
        lead["linkedin_industry"] = company.get("industry")

        if not lead["linkedin_company_url"]:
            continue

        employees = await _scrape_employees(lead["linkedin_company_url"], max_employees=15)
        decision_maker = _pick_best_decision_maker(employees)
        if decision_maker:
            lead["contact_name"] = decision_maker["name"]
            lead["contact_first_name"] = decision_maker["first_name"]
            lead["contact_last_name"] = decision_maker["last_name"]
            lead["contact_title"] = decision_maker["title"]
            lead["contact_linkedin_url"] = decision_maker["linkedin_url"]
            lead["contact_role_tag"] = decision_maker["role_tag"]
            enriched_count += 1

    Actor.log.info(f"LinkedIn enrichment: matched decision maker for {enriched_count}/{len(candidates)}")
    return leads
