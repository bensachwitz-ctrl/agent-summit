"""Lead sourcing — calls the Apify Google Maps Scraper Actor.

Uses `compass/crawler-google-places` (Google Maps Scraper) to pull
business listings for trucking/logging/timber/forestry verticals
across Swamp Fox's licensed states, weighted toward Birmingham, AL.
"""
from __future__ import annotations

import math
from typing import Any

from apify import Actor

GOOGLE_MAPS_ACTOR_ID = "compass/crawler-google-places"

# Birmingham, AL coordinates — used to compute proximity weighting
BIRMINGHAM_LAT = 33.5186
BIRMINGHAM_LON = -86.8104


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km."""
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _industry_tag(query: str) -> str:
    q = query.lower()
    if "log" in q:
        return "logging"
    if "timber" in q or "wood" in q:
        return "timber"
    if "forestry" in q:
        return "forestry"
    return "trucking"


async def source_leads(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """Run Google Maps scraper across all (term × region) combinations."""
    search_terms: list[str] = cfg.get("searchTerms", [])
    regions: list[dict] = cfg.get("regions", [])
    max_per_query: int = int(cfg.get("maxLeadsPerQuery", 25))

    if not search_terms or not regions:
        Actor.log.warning("No search terms or regions provided — nothing to source.")
        return []

    # Build search queries by combining terms + regions
    search_strings = []
    for term in search_terms:
        for region in regions:
            search_strings.append(f"{term} in {region[0]}, {region[1]}")

    Actor.log.info(f"Built {len(search_strings)} search queries across {len(regions)} regions")

    # Call the Google Maps scraper
    run_input = {
        "searchStringsArray": search_strings,
        "maxCrawledPlacesPerSearch": max_per_query,
        "language": "en",
        "scrapeContacts": True,
        "scrapeReviewsCount": 0,
        "scrapeImages": False,
        "skipClosedPlaces": True,
    }

    Actor.log.info(f"Calling {GOOGLE_MAPS_ACTOR_ID} ...")
    run = await Actor.call(GOOGLE_MAPS_ACTOR_ID, run_input=run_input)
    if not run:
        Actor.log.error("Google Maps Actor call returned no run object")
        return []

dataset = await Actor.apify_client.dataset(run.default_dataset_id).list_items()
    raw_items = dataset.items
    Actor.log.info(f"Received {len(raw_items)} raw items from Google Maps")

    # Normalize each result to our internal lead schema
    leads: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    for item in raw_items:
        title = (item.get("title") or "").strip()
        if not title:
            continue

        website = item.get("website")
        phone = item.get("phone") or item.get("phoneUnformatted")
        address = item.get("address")
        city = item.get("city")
        state = item.get("state")
        zip_code = item.get("postalCode")
        location = item.get("location") or {}
        lat = location.get("lat")
        lon = location.get("lng")

        # Dedup key
        dedup_key = (
            (website or "").lower().strip()
            or f"{title.lower()}|{(phone or '').strip()}|{(address or '').lower()}"
        )
        if dedup_key in seen_keys:
            continue
        seen_keys.add(dedup_key)

        # Distance to Birmingham
        distance_km = None
        if lat is not None and lon is not None:
            try:
                distance_km = round(
                    _haversine_km(float(lat), float(lon), BIRMINGHAM_LAT, BIRMINGHAM_LON), 1
                )
            except (TypeError, ValueError):
                distance_km = None

        # Determine industry tag from the search query that produced this item
        # (Google Maps Actor returns `searchString` for each item)
        search_string = item.get("searchString", "") or ""
        industry = _industry_tag(search_string)

        # Pull emails if present (some Google Maps actors expose them via website crawl)
        emails = item.get("emails") or []
        email = emails[0] if emails else None

        leads.append({
            "company_name": title,
            "industry_tag": industry,
            "contact_name": None,  # filled by enrichment
            "email": email,
            "phone": phone,
            "website": website,
            "address": address,
            "city": city,
            "state": state,
            "zip": zip_code,
            "latitude": lat,
            "longitude": lon,
            "distance_to_birmingham_km": distance_km,
            "google_maps_url": item.get("url"),
            "review_count": item.get("reviewsCount") or 0,
            "review_score": item.get("totalScore"),
            "category": item.get("categoryName"),
        })

    Actor.log.info(f"Normalized {len(leads)} unique leads after dedup")
    return leads
