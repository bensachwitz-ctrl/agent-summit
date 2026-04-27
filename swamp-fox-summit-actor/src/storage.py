"""Storage router — fans out lead writes to:
  - Apify Dataset (always)
  - Apify Key-Value store (for state lookups)
  - Google Sheets (if configured)
  - Microsoft Fabric Lakehouse via Delta table (if configured)

Each lead is keyed by `lead_id`. Updates are upserts.
"""
from __future__ import annotations

import io
import json
from datetime import datetime, timezone
from typing import Any

import gspread
import pandas as pd
from apify import Actor
from google.oauth2.service_account import Credentials


SHEETS_HEADER = [
    "lead_id", "company_name", "industry_tag",
    "contact_name", "contact_title", "contact_role_tag", "contact_linkedin_url",
    "email", "email_source", "phone",
    "website", "linkedin_company_url",
    "city", "state", "zip", "distance_to_birmingham_km",
    "yp_years_in_business", "bbb_rating",
    "lead_score", "qualification_status", "producer_assigned",
    "intel_brief",
    "scraped_at",
    "first_email_sent_at", "touch_1_subject_used", "first_email_opened_at", "first_email_clicked_at",
    "second_email_sent_at", "touch_2_subject_used", "second_email_opened_at",
    "third_email_sent_at", "touch_3_subject_used",
    "registered_for_summit", "replied", "reply_received_at", "reply_subject", "reply_body_excerpt",
    "call_booked", "unsubscribed",
    "engagement_status", "last_action_at",
]

KV_INDEX_KEY = "LEADS_INDEX"  # JSON array of lead_ids
KV_LEAD_PREFIX = "LEAD_"      # Per-lead snapshot


class StorageRouter:
    def __init__(self, cfg: dict[str, Any]):
        self.cfg = cfg
        self.dataset = None
        self.kv_store = None
        self.sheet = None
        self.fabric_enabled = bool(cfg.get("fabricEnabled"))
        self._fabric_client = None

    async def initialize(self) -> None:
        self.dataset = await Actor.open_dataset(name="summit_leads")
        self.kv_store = await Actor.open_key_value_store()
        await self._init_sheets()
        if self.fabric_enabled:
            await self._init_fabric()

    # ---------------- Google Sheets ----------------

    async def _init_sheets(self) -> None:
        sheet_id = self.cfg.get("googleSheetId")
        sa_json = self.cfg.get("googleServiceAccountJson")
        if not sheet_id or not sa_json:
            Actor.log.info("Google Sheets sync disabled (missing config).")
            return
        try:
            creds = Credentials.from_service_account_info(
                sa_json,
                scopes=[
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive",
                ],
            )
            client = gspread.authorize(creds)
            self.sheet = client.open_by_key(sheet_id).sheet1
            existing = self.sheet.row_values(1)
            if existing != SHEETS_HEADER:
                self.sheet.update("A1", [SHEETS_HEADER])
            Actor.log.info(f"Google Sheets initialized: {sheet_id}")
        except Exception as e:
            Actor.log.error(f"Sheets init failed: {e}")
            self.sheet = None

    def _row_for_sheet(self, lead: dict[str, Any]) -> list[Any]:
        return [lead.get(k, "") if not isinstance(lead.get(k), bool) else str(lead.get(k))
                for k in SHEETS_HEADER]

    async def _upsert_to_sheets(self, leads: list[dict[str, Any]]) -> None:
        if not self.sheet:
            return
        try:
            existing = self.sheet.get_all_values()
            id_to_row = {row[0]: idx + 1 for idx, row in enumerate(existing[1:], start=1)}
            updates_batch = []
            appends_batch = []
            for lead in leads:
                row = self._row_for_sheet(lead)
                lid = lead["lead_id"]
                if lid in id_to_row:
                    rownum = id_to_row[lid]
                    updates_batch.append({
                        "range": f"A{rownum + 1}:{chr(65 + len(SHEETS_HEADER) - 1)}{rownum + 1}",
                        "values": [row],
                    })
                else:
                    appends_batch.append(row)
            if updates_batch:
                self.sheet.batch_update(updates_batch)
            if appends_batch:
                self.sheet.append_rows(appends_batch, value_input_option="RAW")
            Actor.log.info(f"Sheets sync: {len(updates_batch)} updates, {len(appends_batch)} appends")
        except Exception as e:
            Actor.log.error(f"Sheets sync failed: {e}")

    # ---------------- Microsoft Fabric ----------------

    async def _init_fabric(self) -> None:
        try:
            from azure.identity import ClientSecretCredential
            from azure.storage.filedatalake import DataLakeServiceClient

            tenant = self.cfg.get("azureTenantId")
            client_id = self.cfg.get("azureClientId")
            secret = self.cfg.get("azureClientSecret")
            if not all([tenant, client_id, secret]):
                Actor.log.warning("Fabric enabled but Azure SP creds missing — skipping.")
                self.fabric_enabled = False
                return

            cred = ClientSecretCredential(tenant_id=tenant, client_id=client_id, client_secret=secret)
            workspace = self.cfg.get("fabricWorkspaceName", "SwampFox-Analytics")
            account_url = "https://onelake.dfs.fabric.microsoft.com"
            self._fabric_client = DataLakeServiceClient(account_url=account_url, credential=cred)
            self._fabric_workspace = workspace
            self._fabric_lakehouse = self.cfg.get("fabricLakehouseName", "ClaimsLakehouse")
            self._fabric_table = self.cfg.get("fabricTableName", "summit_outreach_leads")
            Actor.log.info(
                f"Fabric initialized: {workspace}/{self._fabric_lakehouse}.Lakehouse/Tables/{self._fabric_table}"
            )
        except Exception as e:
            Actor.log.error(f"Fabric init failed: {e}")
            self.fabric_enabled = False

    async def _upsert_to_fabric(self, leads: list[dict[str, Any]]) -> None:
        if not self.fabric_enabled or not self._fabric_client:
            return
        try:
            # Build dataframe from leads
            df = pd.DataFrame([{k: lead.get(k) for k in SHEETS_HEADER} for lead in leads])
            df["sync_timestamp"] = datetime.now(timezone.utc).isoformat()

            # Write parquet to OneLake Files area; users can convert to Delta in Fabric
            buf = io.BytesIO()
            df.to_parquet(buf, index=False, engine="pyarrow")
            buf.seek(0)

            fs = self._fabric_client.get_file_system_client(self._fabric_workspace)
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            path = (
                f"{self._fabric_lakehouse}.Lakehouse/Files/summit_outreach/"
                f"leads_{ts}.parquet"
            )
            file_client = fs.get_file_client(path)
            file_client.upload_data(buf.getvalue(), overwrite=True)
            Actor.log.info(f"Fabric sync: wrote {len(df)} rows to {path}")
        except Exception as e:
            Actor.log.error(f"Fabric sync failed: {e}")

    # ---------------- Public API ----------------

    async def upsert_leads(self, leads: list[dict[str, Any]]) -> None:
        # 1. Push everything to dataset (append-only is fine — dedup happens in views)
        for lead in leads:
            await self.dataset.push_data(lead)

        # 2. Maintain KV-store snapshot for fast lookup
        index_obj = await self.kv_store.get_value(KV_INDEX_KEY) or {"ids": []}
        ids = set(index_obj.get("ids", []))
        for lead in leads:
            await self.kv_store.set_value(f"{KV_LEAD_PREFIX}{lead['lead_id']}", lead)
            ids.add(lead["lead_id"])
        await self.kv_store.set_value(KV_INDEX_KEY, {"ids": list(ids)})

        # 3. Sheets
        await self._upsert_to_sheets(leads)

        # 4. Fabric
        await self._upsert_to_fabric(leads)

    async def update_lead(self, lead_id: str, updates: dict[str, Any]) -> None:
        existing = await self.kv_store.get_value(f"{KV_LEAD_PREFIX}{lead_id}")
        if not existing:
            Actor.log.warning(f"update_lead: {lead_id} not found in KV store")
            return
        existing.update(updates)
        existing["last_action_at"] = datetime.now(timezone.utc).isoformat()
        await self.kv_store.set_value(f"{KV_LEAD_PREFIX}{lead_id}", existing)
        await self.dataset.push_data(existing)
        await self._upsert_to_sheets([existing])

    async def fetch_leads(self, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        index_obj = await self.kv_store.get_value(KV_INDEX_KEY) or {"ids": []}
        results = []
        for lid in index_obj.get("ids", []):
            lead = await self.kv_store.get_value(f"{KV_LEAD_PREFIX}{lid}")
            if not lead:
                continue
            if filters and not all(lead.get(k) == v for k, v in filters.items()):
                continue
            results.append(lead)
        return results

    async def all_leads(self) -> list[dict[str, Any]]:
        return await self.fetch_leads(None)
