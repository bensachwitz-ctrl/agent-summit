# Loggers Technology Summit — Outreach Agent v1.3

Multi-source Apify Actor that sources logging, timber, and forestry operators across Swamp Fox Agency's licensed states, identifies decision-makers via LinkedIn, sends personalized 3-touch email sequences with smart subject-line variants, and forwards lead replies + engagement events to Ben with full intelligence briefs attached.

## What's New in v1.3

| Feature | Module |
|---|---|
| Smart subject-line A/B variants per lead | `src/subject_lines.py` |
| Lead intelligence brief generator | `src/lead_intel.py` |
| Reply alerts forwarded to Ben with full context | `src/reply_alerts.py` |
| Engagement alerts (clicks + registrations) | `src/web_server.py` (extended) |
| Reply body capture + parsing | `src/reply_monitor.py` (rewritten) |
| Cleaner professional copy throughout | `src/templates.py` |

## The Event

| Field | Detail |
|---|---|
| Name | 4th Annual Loggers Technology Summit |
| Day 1 | Aug 28, 2026 — 6:00 PM CT — Welcome Reception @ Alabama Sports Hall of Fame + Pinnacle Award |
| Day 2 | Aug 29, 2026 — Full Summit @ Barber Motorsports Park |
| Hotel | Westin Birmingham (221 Richard Arrington Jr Blvd N) |
| Audience | Logging owners, forestry fleet operators, safety/ops leaders, forestry tech partners |
| URL | https://swampfoxagency.com/the-summit/ |

## Multi-Source Lead Pipeline

```
Phase 1: Google Maps          → raw business listings (compass/crawler-google-places)
Phase 2: LinkedIn             → company page + decision maker employee
                                (harvestapi/linkedin-company + linkedin-company-employees)
Phase 3: Yellow Pages + BBB   → years in business, rating, alt phone
Phase 3: Apify Contact Scraper → deep email/phone/social extraction
Phase 4: Website Crawl        → mailto + regex email extraction
Phase 5: Email Pattern Finder → {first}.{last}@domain when name + domain known
                                (with MX validation)
Phase 6: Qualification        → score + producer routing + status assignment
Phase 7: Triple-Write Storage → Apify Dataset + Google Sheet + Fabric Lakehouse
Phase 8: Outreach (if mode)   → Touch-1 via Gmail SMTP with tracking
```

## Decision Maker Identification (LinkedIn)

The Actor scrapes each company's LinkedIn page and pulls employees with these titles, ranked:

| Priority | Title Match | Score |
|---|---|---|
| 1 | Owner / Founder / President / CEO | 100 |
| 2 | VP Operations / VP Fleet / VP Safety | 85 |
| 3 | Fleet Manager / Director | 80 |
| 4 | Safety Manager / Director | 75 |
| 5 | Operations Manager | 70 |
| 6 | General Manager | 65 |
| 7 | Risk Manager | 60 |
| 8 | Transport Manager | 55 |

The highest-scoring contact is merged into the lead record. Touch emails personalize the salutation by first name and inject a role-specific second sentence (e.g., a Fleet Manager gets a different opener than an Owner).

## Email Pattern Generation

When LinkedIn finds a name but website crawl finds no email, the Actor generates the most-common email patterns ranked by industry frequency:

| Rank | Pattern | Approx. Frequency |
|---|---|---|
| 1 | `first.last@domain` | 35% |
| 2 | `first@domain` | 20% |
| 3 | `firstlast@domain` | 10% |
| 4 | `flast@domain` | 10% |
| 5 | `first_last@domain` | 5% |

MX records are validated before any pattern is used. Guessed emails are flagged `email_source=pattern_guess` and lose 5 points in scoring so you can review in Sheets before sending.

## Tracking Architecture

The Actor runs an aiohttp web server on port 4321 (auto-exposed at the Apify Web Server URL) serving:

| Endpoint | Purpose |
|---|---|
| `GET /t/open/{lead_id}/{touch}.gif` | Records open + returns 1×1 GIF |
| `GET /t/click/{lead_id}/{touch}?u=URL` | Records click + 302 redirects |
| `GET /t/unsub/{lead_id}` | Marks unsubscribed + returns confirmation page |
| `GET /t/registered/{lead_id}` | External webhook to mark registered (call from form thank-you page) |
| `GET /health` | Liveness probe |

After sends, the server stays alive for `trackingKeepAliveMinutes` (default 10) to capture immediate engagement.

## Reply Detection

In `metrics_sync` and `follow_up_only` modes, the Actor connects to Gmail via IMAP, searches the inbox for replies from any contacted lead address, and auto-marks `replied=True`. This suppresses further follow-ups for that lead.

## Email Sequence

| Touch | Day | Subject Variant Examples | Focus |
|---|---|---|---|
| 1 | 0 | `{first}, sending anyone to the Loggers Summit Aug 28–29?` / `{state} logging operators heading to Birmingham Aug 28–29` / `Day 2 fleet content at the Loggers Summit — {company}` | Networking + industry tracks + register CTA |
| 2 | 4 | `{first} — Pinnacle Award + Day 1 reception details` / `Following up on the Loggers Summit — quick note for {company}` | Reception value + Pinnacle Award + offer call |
| 3 | 10 | `{first} — closing Loggers Summit registration` / `Headcount cutoff: Loggers Summit Aug 28–29` | Deadline + soft fallback |

Each touch has 3-5 subject variants. Selection is deterministic per-lead based on attributes (first name, role, location), so the same lead always sees the same variant for consistency. Variant ID is tracked in the dataset for open-rate analysis.

## Lead Intelligence Briefs

After qualification, every lead gets an `intel_brief` field — a sales-ready profile pulling from all enrichment sources. Visible in Sheets, Fabric, and attached to reply alerts.

Brief contents:
- **Operating context**: location, distance to Birmingham, years in business, employee count, BBB rating
- **Decision maker**: name, title, LinkedIn URL
- **Contact signals**: email source quality, phone, website
- **Role-specific talking points**: tailored to Owner / Fleet Manager / Safety Lead / etc.
- **Industry hooks**: logging / timber / forestry-specific conversation starters
- **Engagement history**: when sent, opened, clicked, registered
- **Suggested next action**: based on engagement state

## Reply + Engagement Alerts

When leads engage, Ben gets notification emails at `alertNotifyEmail` (default: `workbenjaminsachwitz@gmail.com`) with full intel context:

| Event | Alert Subject | Contents |
|---|---|---|
| Lead replies | `🔔 Reply from {Contact} — {Company}` | Full reply body + intel brief + suggested action |
| Lead clicks register link | `🔗 Click: {Company} — engaged with Summit registration link` | Intel brief + suggested action |
| Lead registers | `✅ REGISTERED: {Company} — Loggers Summit` | Intel brief + welcome action steps |
| Lead opens email | (no alert — too noisy, tracked silently in dataset) | — |

This means when someone replies to a Touch email, Ben gets an internal notification with everything he needs to follow up personally within minutes — original reply, who they are, role-specific talking points, and the recommended action.

## Run Modes

| Mode | What It Does | When |
|---|---|---|
| `source_only` | Full pipeline, no emails | First run, weekly source refresh |
| `source_and_outreach` | Full pipeline + Touch-1 | Aggressive launch |
| `outreach_only` | Touch-1 to existing dataset | After manual review in Sheets |
| `follow_up_only` | Reply check → Touch-2 → Touch-3 | Daily cron |
| `metrics_sync` | Reply check + Sheets/Fabric refresh | Hourly |

## Recommended Schedule

| Cron | Mode | Purpose |
|---|---|---|
| `0 6 * * MON` | `source_only` | Weekly source new leads |
| Manual | `outreach_only` | Send Touch-1 after Sheet review |
| `0 9 * * *` | `follow_up_only` | Daily Touch-2/Touch-3 + reply check |
| `0 * * * *` | `metrics_sync` | Hourly dashboard refresh |

## Setup

### 1. Push to Apify

```bash
npm i -g apify-cli
cd swamp-fox-summit-actor
apify login
apify push
```

### 2. Gmail App Password

`myaccount.google.com → Security → 2-Step Verification → App passwords` → name "Apify Summit Outreach" → save 16-char password.

### 3. Google Sheet (recommended)

1. Create sheet **Loggers Summit Outreach Leads**
2. Google Cloud Console → service account → download JSON key
3. Share sheet with service account email (Editor)
4. Paste JSON into `googleServiceAccountJson`, sheet ID into `googleSheetId`

### 4. Microsoft Fabric

1. Create service principal with **OneLake Data Contributor** on workspace `SwampFox-Analytics`
2. Grant Read/Write on `ClaimsLakehouse`
3. Provide `azureTenantId`, `azureClientId`, `azureClientSecret`
4. Run this Spark notebook cell once to register the Delta table:

```python
df = spark.read.parquet("Files/summit_outreach/")
df.write.format("delta").mode("overwrite").saveAsTable("summit_outreach_leads")
```

### 5. Registration Form Confirmation Webhook

Add this to the registration form's thank-you page so the Actor knows when leads register:

```html
<img src="https://YOUR-ACTOR-URL.apify.actor/t/registered/{LEAD_ID}" width="1" height="1" />
```

Replace `{LEAD_ID}` with the lead_id passed in the URL query when they clicked through (`?lid=...`).

## File Structure

```
swamp-fox-summit-actor/
├── .actor/
│   ├── actor.json
│   ├── input_schema.json
│   └── dataset_schema.json
├── src/
│   ├── __init__.py
│   ├── __main__.py
│   ├── main.py                 # Pipeline orchestrator
│   ├── lead_sourcing.py        # Google Maps Actor
│   ├── linkedin.py             # LinkedIn company + employees
│   ├── multi_source.py         # Yellow Pages + BBB + contact scraper
│   ├── enrichment.py           # Website email crawl
│   ├── email_finder.py         # Pattern generation + MX validation
│   ├── qualification.py        # Scoring + producer routing
│   ├── outreach.py             # Gmail SMTP + rate limiting
│   ├── templates.py            # 3-touch email templates
│   ├── storage.py              # Dataset + Sheets + Fabric
│   ├── tracking.py             # Metrics sync helpers
│   ├── web_server.py           # aiohttp tracking endpoints
│   └── reply_monitor.py        # Gmail IMAP reply detection
├── Dockerfile
├── requirements.txt
└── README.md
```

## Cost Estimate

| Item | Cost per Run |
|---|---|
| Google Maps Scraper | ~$7 / 1,000 places |
| LinkedIn Company + Employees | ~$10 / 100 companies |
| Apify Contact Scraper | ~$5 / 200 sites |
| Yellow Pages + BBB | Free (HTTP) |
| Email Pattern Finder | Free (DNS only) |
| Apify compute | ~$0.05 / hour |
| Gmail SMTP | Free |

Typical run: 20 regions × 10 search terms × 25 leads ≈ 5,000 raw → ~2,500 unique → 200 LinkedIn enriched → **~$25–35 per full source run**.

## Compliance Pre-Production Checklist

- [ ] Update `SWAMP_FOX_FOOTER` in `templates.py` with Swamp Fox office address (CAN-SPAM)
- [ ] Add Ben's NPN / state license # to footer (insurance solicitation laws)
- [ ] Verify `registrationFormUrl` points to the actual form anchor or page
- [ ] Run `mode=source_only`, `dryRun=true`, `maxLeadsPerQuery=10` first
- [ ] Inspect Apify Dataset → verify decision-maker matches are accurate
- [ ] Connect Sheet → run again → verify all columns populate
- [ ] Connect Fabric SP → run again → verify Parquet file lands
- [ ] Run `mode=source_and_outreach`, `dryRun=true` → review email previews
- [ ] Flip `dryRun=false` for 5-lead test (use your own emails)
- [ ] Schedule production runs

## Stop Conditions / Safety Caps

| Condition | Action |
|---|---|
| Lead replies | Auto `replied=True`, no further follow-ups |
| Lead unsubscribes | `unsubscribed=True`, suppressed forever |
| Lead clicks register link | `registered_for_summit=True`, no further follow-ups |
| Email pattern is guessed | `-5` score, flagged for review before send |
| Out-of-licensed-state | Excluded from qualified pool |
| Gmail SMTP error | Logged, lead skipped, run continues |
| Send rate | Hard cap 20/min |
