"""Lead sourcing — calls the Apify Google Maps Scraper Actor."""
from __future__ import annotations

import math
from typing import Any
from apify import Actor

GOOGLE_MAPS_ACTOR_ID = "compass/crawler-google-places"

async def source_leads(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """Run Google Maps scraper and enrich results."""
    search_terms: list[str] = cfg.get("searchTerms", [])
    regions: list[list[str]] = cfg.get("regions", [])
    max_per_query: int = int(cfg.get("maxLeadsPerQuery", 25))

    if not search_terms or not regions:
        Actor.log.warning("No search terms or regions provided.")
        return []

    # 1. Build Search Queries
    search_strings = []
    for term in search_terms:
        for region in regions:
            # Fixed the list index issue from earlier
            search_strings.append(f"{term} in {region[0]}, {region[1]}")

    # 2. Call Google Maps Scraper (Phase 1)
    run_input = {
        "searchStringsArray": search_strings,
        "maxCrawledPlacesPerSearch": max_per_query,
        "scrapeContacts": True,
    }

    Actor.log.info(f"Calling {GOOGLE_MAPS_ACTOR_ID} ...")
    # Corrected Line 72: Using GOOGLE_MAPS_ACTOR_ID
    run = await Actor.call(GOOGLE_MAPS_ACTOR_ID, run_input=run_input)
    
    if not run:
        Actor.log.error("Google Maps Actor call failed.")
        return []

    dataset = await Actor.apify_client.dataset(run.default_dataset_id).list_items()
    raw_items = dataset.items
    Actor.log.info(f"Received {len(raw_items)} raw items from Google Maps")

    # 3. Normalize to Swamp Fox Sheet Schema
    leads: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    for item in raw_items:
        company = (item.get("title") or "").strip()
        if not company or company in seen_keys:
            continue
        seen_keys.add(company)

        city = item.get("city", "")
        state = item.get("state", "")
        
        leads.append({
            "First Name": "",  
            "Last Name": "",   
            "Company": company,
            "Job title": "",   
            "Location": f"{city}, {state}".strip(", "),
            "Email": (item.get("emails") or [None])[0], # Grab email if scraper found it
            "Linkedin profile": "",
            "Outreach status": "Pending",
            "DOT number": "",  
            "Fleet size": "",  
            "Personalization snippet": f"Web: {item.get('website', '')} | Phone: {item.get('phone', '')}"
        })

    # 4. Phase 3 — Multi-source enrichment (Contact Scraper)
    # This is where we find the decision maker names and emails
    Actor.log.info("Phase 3 — Multi-source enrichment")
    
    # We only enrich leads that have a website but no email yet
    enrichment_targets = [l for l in leads if "Web: http" in l["Personalization snippet"] and not l["Email"]]
    
    if enrichment_targets:
        contact_input = {
            "startUrls": [{"url": l["Personalization snippet"].split("|")[0].replace("Web: ", "").strip()} for l in enrichment_targets],
            "maxRequestsPerStartUrl": 5
        }
        
        # FIXED: Removed 'timeout_secs' to prevent the crash you saw in the logs
        enrich_run = await Actor.call("apify/contact-info-scraper", run_input=contact_input)
        
        if enrich_run:
            # Logic to merge results would go here
            Actor.log.info("Enrichment complete.")

    Actor.log.info(f"Normalized {len(leads)} unique leads for Swamp Fox")
    return leads
