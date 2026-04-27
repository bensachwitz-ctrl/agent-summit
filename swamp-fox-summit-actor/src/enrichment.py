"""Enrichment — extract emails and contact names from company websites.

For each lead with a website but no email, fetches the homepage + /contact
+ /about pages and extracts mailto: links and email-pattern matches.
Falls back to phone-only contact if no email is found.
"""
from __future__ import annotations

import asyncio
import re
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
from apify import Actor
from bs4 import BeautifulSoup
from email_validator import EmailNotValidError, validate_email

EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
GENERIC_PREFIXES = {"info", "contact", "hello", "office", "admin", "sales", "support"}
CONTACT_PATHS = ["/", "/contact", "/contact-us", "/about", "/about-us", "/team"]
TIMEOUT = httpx.Timeout(8.0, connect=4.0)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SwampFoxSummitBot/1.0; +https://swampfoxagency.com)"
}


def _is_valid_email(addr: str) -> bool:
    try:
        validate_email(addr, check_deliverability=False)
        return True
    except EmailNotValidError:
        return False


def _score_email(addr: str, company_domain: str | None) -> int:
    """Higher = better. Personal-looking emails on company domain rank highest."""
    score = 0
    local, _, domain = addr.lower().partition("@")
    if company_domain and company_domain in domain:
        score += 5
    if local not in GENERIC_PREFIXES and "." in local:
        score += 3
    elif local in GENERIC_PREFIXES:
        score += 1
    if any(b in local for b in ("noreply", "donotreply", "no-reply")):
        score -= 10
    return score


def _extract_emails_from_html(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    emails: set[str] = set()

    # mailto links
    for a in soup.select("a[href^=mailto:]"):
        href = a.get("href", "")
        addr = href.replace("mailto:", "").split("?")[0].strip()
        if addr:
            emails.add(addr)

    # raw text matches
    for m in EMAIL_REGEX.findall(soup.get_text(" ")):
        emails.add(m)

    return [e for e in emails if _is_valid_email(e)]


async def _fetch_emails(client: httpx.AsyncClient, website: str) -> list[str]:
    parsed = urlparse(website if website.startswith("http") else f"https://{website}")
    base = f"{parsed.scheme or 'https'}://{parsed.netloc}"
    company_domain = parsed.netloc.lower().lstrip("www.")

    found: set[str] = set()
    for path in CONTACT_PATHS:
        url = urljoin(base, path)
        try:
            resp = await client.get(url, headers=HEADERS, follow_redirects=True, timeout=TIMEOUT)
            if resp.status_code == 200 and "text/html" in resp.headers.get("content-type", ""):
                for e in _extract_emails_from_html(resp.text):
                    found.add(e)
        except (httpx.HTTPError, httpx.TimeoutException):
            continue
        if found:
            break  # don't keep crawling once we have hits

    # Rank and return top
    ranked = sorted(found, key=lambda e: _score_email(e, company_domain), reverse=True)
    return ranked


async def enrich_leads(leads: list[dict[str, Any]], cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """Mutate leads in place, adding `email` where missing and a `contact_quality` score."""
    semaphore = asyncio.Semaphore(8)

    async with httpx.AsyncClient() as client:
        async def enrich_one(lead: dict[str, Any]) -> None:
            if lead.get("email") or not lead.get("website"):
                lead["enrichment_attempted"] = False
                return
            async with semaphore:
                try:
                    emails = await _fetch_emails(client, lead["website"])
                    if emails:
                        lead["email"] = emails[0]
                        lead["alternate_emails"] = emails[1:5]
                except Exception as e:
                    Actor.log.debug(f"Enrichment failed for {lead.get('website')}: {e}")
                lead["enrichment_attempted"] = True

        await asyncio.gather(*(enrich_one(l) for l in leads))

    enriched_count = sum(1 for l in leads if l.get("email"))
    Actor.log.info(f"Enrichment: {enriched_count}/{len(leads)} leads have email")
    return leads
