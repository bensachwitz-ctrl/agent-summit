"""Email finder — generates likely email patterns and validates via MX lookup.

For leads that have a contact name + company website but no email, we generate
the most common patterns ({first}.{last}@domain, {first}@domain, {f}{last}@domain, etc.)
and rank them by frequency. We validate the domain has a valid MX record before
treating any pattern as usable.
"""
from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import urlparse

from apify import Actor

try:
    import dns.resolver  # type: ignore
    DNS_AVAILABLE = True
except ImportError:
    DNS_AVAILABLE = False


def _domain_from_website(website: str | None) -> str | None:
    if not website:
        return None
    parsed = urlparse(website if website.startswith("http") else f"https://{website}")
    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return netloc or None


def _generate_patterns(first: str, last: str, domain: str) -> list[str]:
    """Return likely email addresses ranked by population frequency.

    Order roughly matches industry patterns from email validation services
    (Hunter, Snov, Apollo aggregate stats).
    """
    f = first.lower().strip()
    l = last.lower().strip()
    if not f or not l or not domain:
        return []
    patterns = [
        f"{f}.{l}@{domain}",      # ~35% — most common at small-mid biz
        f"{f}@{domain}",          # ~20%
        f"{f}{l}@{domain}",       # ~10%
        f"{f[0]}{l}@{domain}",    # ~10%
        f"{f}_{l}@{domain}",      # ~5%
        f"{f}-{l}@{domain}",      # ~3%
        f"{l}@{domain}",          # ~3%
        f"{l}.{f}@{domain}",      # ~2%
        f"{f[0]}.{l}@{domain}",   # ~2%
    ]
    # dedupe
    seen, out = set(), []
    for p in patterns:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


async def _has_valid_mx(domain: str) -> bool:
    """Check if domain has at least one MX record (sync DNS in thread)."""
    if not DNS_AVAILABLE:
        return True  # assume ok if dns lib not installed
    try:
        def _query():
            try:
                answers = dns.resolver.resolve(domain, "MX", lifetime=5.0)
                return len(answers) > 0
            except Exception:
                return False
        return await asyncio.to_thread(_query)
    except Exception:
        return False


async def find_emails(leads: list[dict[str, Any]], cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """For leads with name + website but no email, generate guessed emails."""
    if not cfg.get("enableEmailFinder", True):
        return leads

    candidates = [
        l for l in leads
        if not l.get("email")
        and l.get("contact_first_name")
        and l.get("contact_last_name")
        and l.get("website")
    ]
    Actor.log.info(f"Email finder running on {len(candidates)} leads")

    # Cache MX lookups per domain
    domain_mx_cache: dict[str, bool] = {}
    found = 0
    for lead in candidates:
        domain = _domain_from_website(lead["website"])
        if not domain:
            continue
        if domain not in domain_mx_cache:
            domain_mx_cache[domain] = await _has_valid_mx(domain)
        if not domain_mx_cache[domain]:
            lead["email_guess_status"] = "no_mx"
            continue

        guesses = _generate_patterns(lead["contact_first_name"], lead["contact_last_name"], domain)
        if not guesses:
            continue

        # Without paid SMTP/Hunter validation, take the highest-frequency pattern
        # but flag it as guessed so outreach can decide whether to send.
        lead["email"] = guesses[0]
        lead["email_source"] = "pattern_guess"
        lead["email_guess_alternates"] = guesses[1:5]
        found += 1

    Actor.log.info(f"Email finder generated patterns for {found} leads")
    return leads
