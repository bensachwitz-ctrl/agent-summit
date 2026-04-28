"""Multi-source enrichment — Yellow Pages, BBB, contact scraper.

Hardened: each source can fail independently without crashing the run.
The Apify contact-info-scraper is optional; only HTTP-based sources
(Yellow Pages, BBB) are guaranteed.
"""
from __future__ import annotations

import asyncio
import re
from typing import Any
from urllib.parse import quote_plus, urlparse

import httpx
from apify import Actor
from bs4 import BeautifulSoup

CONTACT_SCRAPER_ACTOR = "vdrmota/contact-info-scraper"

TIMEOUT = httpx.Timeout(10.0, connect=5.0)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SwampFoxSummitBot/1.0; +https://swampfoxagency.com)",
    "Accept": "text/html,application/xhtml+xml",
}

EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_REGEX = re.compile(r"\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}")


async def _yellow_pages_search(client: httpx.AsyncClient, name: str, location: str) -> dict[str, Any]:
    url = f"https://www.yellowpages.com/search?search_terms={quote_plus(name)}&geo_location_terms={quote_plus(location)}"
    result: dict[str, Any] = {"yp_url": None, "yp_phone": None, "yp_years_in_business": None}
    try:
        resp = await client.get(url, headers=HEADERS, timeout=TIMEOUT, follow_redirects=True)
        if resp.status_code != 200:
            return result
        soup = BeautifulSoup(resp.text, "html.parser")
        first = soup.select_one("div.result")
        if not first:
            return result
        link = first.select_one("a.business-name")
        if link and link.get("href"):
            result["yp_url"] = "https://www.yellowpages.com" + link["href"]
        phone = first.select_one(".phones")
        if phone:
            m = PHONE_REGEX.search(phone.get_text())
            if m:
                result["yp_phone"] = m.group(0)
        years = first.select_one(".years-in-business strong")
        if years:
            try:
                result["yp_years_in_business"] = int(re.search(r"\d+", years.get_text()).group(0))
            except (AttributeError, ValueError):
                pass
    except (httpx.HTTPError, httpx.TimeoutException) as e:
        Actor.log.debug(f"Yellow Pages lookup failed for {name}: {e}")
    return result


async def _bbb_search(client: httpx.AsyncClient, name: str, location: str) -> dict[str, Any]:
    result = {"bbb_rating": None, "bbb_accredited": None, "bbb_url": None}
    try:
        url = f"https://www.bbb.org/search?find_country=USA&find_text={quote_plus(name)}&find_loc={quote_plus(location)}"
        resp = await client.get(url, headers=HEADERS, timeout=TIMEOUT, follow_redirects=True)
        if resp.status_code != 200:
            return result
        soup = BeautifulSoup(resp.text, "html.parser")
        first = soup.select_one("div.result-card") or soup.select_one("article")
        if not first:
            return result
        rating_el = first.find(string=re.compile(r"BBB Rating"))
        if rating_el:
            m = re.search(r"BBB Rating[:\s]+([A-F][+\-]?)", str(rating_el))
            if m:
                result["bbb_rating"] = m.group(1)
        if first.find(string=re.compile(r"BBB Accredited", re.I)):
            result["bbb_accredited"] = True
    except (httpx.HTTPError, httpx.TimeoutException):
        pass
    return result


async def _contact_scraper_actor(websites: list[str]) -> dict[str, dict[str, Any]]:
    if not websites:
        return {}
    try:
        run = await Actor.call(
            CONTACT_SCRAPER_ACTOR,
            run_input={
                "startUrls": [{"url": w} for w in websites],
                "maxDepth": 2,
                "maxRequests": len(websites) * 5,
            },
            timeout_secs=600,
        )
        if not run:
            return {}
        items = (await Actor.apify_client.dataset(run["defaultDatasetId"]).list_items()).items
        result: dict[str, dict[str, Any]] = {}
        for item in items:
            domain = urlparse(item.get("url", "")).netloc.lower().lstrip("www.")
            if not domain:
                continue
            entry = result.setdefault(domain, {"emails": [], "phones": [], "social": {}})
            for e in (item.get("emails") or []):
                if e not in entry["emails"]:
                    entry["emails"].append(e)
            for p in (item.get("phones") or []):
                if p not in entry["phones"]:
                    entry["phones"].append(p)
            for k in ("linkedin", "facebook", "twitter", "instagram"):
                v = item.get(f"{k}Urls") or item.get(k)
                if v and not entry["social"].get(k):
                    entry["social"][k] = v if isinstance(v, str) else (v[0] if v else None)
        return result
    except Exception as e:
        Actor.log.warning(
            f"Contact-info-scraper unavailable: {e}. "
            f"Skipping deep contact scrape. To enable, subscribe to "
            f"'{CONTACT_SCRAPER_ACTOR}' free in Apify Store."
        )
        return {}


async def enrich_multi_source(leads: list[dict[str, Any]], cfg: dict[str, Any]) -> list[dict[str, Any]]:
    if not cfg.get("enableMultiSource", True):
        Actor.log.info("Multi-source enrichment disabled.")
        return leads

    semaphore = asyncio.Semaphore(6)
    websites = [l["website"] for l in leads if l.get("website")]

    contact_task = asyncio.create_task(_contact_scraper_actor(websites[:200]))

    async with httpx.AsyncClient() as client:
        async def enrich_one(lead: dict[str, Any]) -> None:
            async with semaphore:
                location = f"{lead.get('city','')}, {lead.get('state','')}".strip(", ")
                yp = await _yellow_pages_search(client, lead["company_name"], location)
                bbb = await _bbb_search(client, lead["company_name"], location)
                lead.update(yp)
                lead.update(bbb)
                if not lead.get("phone") and yp.get("yp_phone"):
                    lead["phone"] = yp["yp_phone"]

        await asyncio.gather(*(enrich_one(l) for l in leads))

    contact_data = await contact_task
    matched_contacts = 0
    for lead in leads:
        if not lead.get("website"):
            continue
        domain = urlparse(lead["website"]).netloc.lower().lstrip("www.")
        deep = contact_data.get(domain)
        if not deep:
            continue
        matched_contacts += 1
        all_emails = ([lead["email"]] if lead.get("email") else []) + deep["emails"]
        unique_emails = []
        for e in all_emails:
            if e and e not in unique_emails:
                unique_emails.append(e)
        if unique_emails:
            lead["email"] = lead.get("email") or unique_emails[0]
            lead["alternate_emails"] = unique_emails[1:6]
        if deep["phones"]:
            lead.setdefault("alternate_phones", deep["phones"][:3])
        if deep["social"]:
            lead["social_links"] = deep["social"]

    Actor.log.info(f"Multi-source enrichment matched contact data for {matched_contacts} leads")
    return leads
