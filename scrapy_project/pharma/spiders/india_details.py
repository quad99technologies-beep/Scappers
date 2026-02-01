#!/usr/bin/env python3
"""
India NPPA Scrapy Spider -- parallel work-queue variant.

Each spider instance (worker) does:
1. Warm-up GET to searchMedicine (establish cookies)
2. GET formulationListNew -- build formulation ID map
3. Claim a batch of pending formulations from formulation_status table
4. For each claimed formulation:
   a. GET formulationDataTableNew -- list of SKUs
   b. For each SKU: GET skuMrpNew, otherBrandPriceNew, medDtlsNew
   c. Write all data to PostgreSQL
5. Repeat step 3 until no pending formulations remain

Business logic is IDENTICAL to the original 02_get_details.py.
Coordination: formulation_status table acts as a work queue.
Workers claim batches atomically so no formulation is scraped twice.
"""

import hashlib
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import scrapy
from scrapy import Request, Spider
from scrapy.http import TextResponse

# Ensure repo root on path for core imports
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.db.postgres_connection import PostgresDB
from core.db.models import apply_common_schema, run_ledger_finish
from core.db.schema_registry import SchemaRegistry
from core.db.upsert import upsert_items

logger = logging.getLogger(__name__)

# --- Constants (same as original 02_get_details.py) ---
SEARCH_URL = "https://nppaipdms.gov.in/NPPA/PharmaSahiDaam/searchMedicine"
REST_BASE = "https://nppaipdms.gov.in/NPPA/rest"
API_FORMULATION_LIST = f"{REST_BASE}/formulationListNew"
API_FORMULATION_TABLE = f"{REST_BASE}/formulationDataTableNew"
API_SKU_MRP = f"{REST_BASE}/skuMrpNew"
API_OTHER_BRANDS = f"{REST_BASE}/otherBrandPriceNew"
API_MED_DTLS = f"{REST_BASE}/medDtlsNew"

# How many formulations each worker claims per batch
CLAIM_BATCH_SIZE = int(os.getenv("CLAIM_BATCH_SIZE", os.getenv("INDIA_CLAIM_BATCH", "10")))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))


# --- Utility functions (identical to original) ---

def _sanitize_api_value(value: Any) -> str:
    if value is None:
        return ""
    s = str(value)
    s = s.replace(" ", "_").replace("\r\n", "_").replace("\n", "_").replace("\r", "_")
    if s == "undefined":
        return ""
    return s


def compute_fhttf(params: Dict[str, Any]) -> str:
    """Compute the MD5 auth token required by NPPA REST endpoints."""
    entries = sorted(
        ((k.lower(), k, _sanitize_api_value(params.get(k))) for k in params if k != "fhttf"),
        key=lambda x: x[0],
    )
    acc = "".join(v for _, __, v in entries)
    return hashlib.md5(acc.encode("utf-8")).hexdigest()


def normalize_name(value: str) -> str:
    return " ".join((value or "").strip().upper().split())


def sget(record: Dict[str, Any], key: str) -> str:
    value = record.get(key, "")
    return "" if value is None else str(value)


def safe_json(response) -> Any:
    """Parse JSON from response, handling non-UTF-8 bytes (e.g. 0xa0 from NPPA)."""
    try:
        return response.json()
    except (UnicodeDecodeError, ValueError):
        pass

    try:
        text = response.body.decode("latin-1", errors="replace")
        text = text.strip()
        if not text:
            return None
        return json.loads(text)
    except Exception:
        return None


def build_api_url(endpoint: str, params: Dict[str, Any]) -> str:
    """Build URL with fhttf token appended to query params."""
    params = dict(params)
    params["fhttf"] = compute_fhttf(params)
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{endpoint}?{qs}"


class IndiaNPPASpider(Spider):
    """
    Scrapy spider for India NPPA medicine details.

    Custom settings override global to match NPPA's rate limits.
    Scrapy handles retry (429, 500-504) and autothrottle automatically.
    """

    name = "india_details"
    country_name = "India"
    allowed_domains = ["nppaipdms.gov.in"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.8,
        "CONCURRENT_REQUESTS": 1,  # Sequential per formulation (API requires session state)
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1.0,
        "RETRY_TIMES": MAX_RETRIES,
        "RETRY_HTTP_CODES": [429, 500, 502, 503, 504],
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        },
        # Disable the generic pipeline -- this spider writes directly to DB
        "ITEM_PIPELINES": {},
        # Override middlewares to use platform fetcher + DB logging
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
            "pharma.middlewares.OTelDownloaderMiddleware": 50,
            "pharma.middlewares.RandomUserAgentMiddleware": 90,
            "pharma.middlewares.BrowserHeadersMiddleware": 95,
            "pharma.middlewares.HumanizeDownloaderMiddleware": 100,
            "pharma.middlewares.AntiBotDownloaderMiddleware": 110,
            "pharma.middlewares.ProxyRotationMiddleware": 120,
            "pharma.middlewares.PlatformFetcherMiddleware": 130,
        },
    }

    def __init__(self, run_id=None, worker_id=1, limit=None, platform_run_id=None, **kwargs):
        super().__init__(**kwargs)

        self.run_id = run_id
        self.worker_id = int(worker_id)
        self.limit = int(limit) if limit else None
        self.formulation_map: Dict[str, str] = {}
        self.platform_run_id = platform_run_id or os.getenv("PLATFORM_RUN_ID") or None

        # DB handle
        self.db: Optional[PostgresDB] = None

        # Stats
        self.stats_medicines = 0
        self.stats_substitutes = 0
        self.stats_errors = 0
        self.stats_completed = 0
        self.stats_zero = 0
        
        # PERFORMANCE FIX: Track last performance log time
        self._last_perf_log = 0
        self._perf_log_interval = 50  # Log every 50 formulations

        # Track pending detail requests per formulation for deferred completion
        # key=formulation, value={"pending": int, "sku_count": int}
        self._pending_details: Dict[str, Dict] = {}
        # Number of formulations in current batch still awaiting detail completion
        self._batch_pending_formulations = 0
        # Platform entity mapping: hidden_id -> entity_id
        self._entity_ids: Dict[str, int] = {}
        self._platform_ready = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start_requests(self):
        """Step 1: Warm-up GET to establish cookies, then fetch formulation list."""
        yield Request(
            url=SEARCH_URL,
            callback=self._after_warmup,
            dont_filter=True,
            meta={"handle_httpstatus_list": [200, 302]},
        )

    def _after_warmup(self, response: TextResponse):
        """Step 2: Fetch formulation list to build ID map."""
        self.logger.info("[W%d] Session established. Fetching formulation list...", self.worker_id)
        yield Request(
            url=API_FORMULATION_LIST,
            callback=self._parse_formulation_list,
            headers={"Referer": SEARCH_URL},
            dont_filter=True,
            meta={"source": "api", "entity_type": "formulation_list"},
        )

    def _parse_formulation_list(self, response: TextResponse):
        """Step 3: Parse formulation map, then start claiming work."""
        items = safe_json(response)
        if not isinstance(items, list) or not items:
            self.logger.error("[W%d] Formulation list response was not JSON list (status=%s)", self.worker_id, response.status)
            raise scrapy.exceptions.CloseSpider("formulation_list_invalid")
        if isinstance(items, list):
            for row in items:
                if not isinstance(row, dict):
                    continue
                name = normalize_name(row.get("formulationName", ""))
                fid = str(row.get("formulationId", "")).strip()
                if name and fid:
                    self.formulation_map[name] = fid

        self.logger.info("[W%d] Loaded %d formulations from API map", self.worker_id, len(self.formulation_map))

        # Initialize DB connection
        self._init_db()
        self._ensure_platform()

        # Get total formulations count for progress reporting
        cur = self.db.execute("SELECT COUNT(*) FROM in_formulation_status WHERE run_id = %s", (self.run_id,))
        self.total_formulations = cur.fetchone()[0] or 0

        # Claim first batch and start processing
        yield from self._claim_and_process()

    def _claim_and_process(self):
        """Claim a batch of pending formulations from the queue and yield requests."""
        while True:
            batch = self._claim_batch()
            if not batch:
                self.logger.info("[W%d] No more pending formulations. Done.", self.worker_id)
                return

            self.logger.info("[W%d] Claimed %d formulations", self.worker_id, len(batch))
            print(f"[DB] W{self.worker_id} | CLAIM | {len(batch)} formulations from queue", flush=True)

            actionable: List[tuple[str, str]] = []
            for formulation in batch:
                fid = self.formulation_map.get(normalize_name(formulation))
                if not fid:
                    self.logger.warning("[W%d] '%s' not in API map, skipping", self.worker_id, formulation)
                    self._mark_formulation(formulation, "zero_records")
                    self.stats_zero += 1
                    continue
                actionable.append((formulation, fid))

            if not actionable:
                continue

            # Set batch counter — next batch claimed only when all formulations' details complete
            self._batch_pending_formulations = len(actionable)

            for i, (formulation, fid) in enumerate(actionable):
                params = {"formulationId": fid, "strengthId": "0", "dosageId": "0"}
                url = build_api_url(API_FORMULATION_TABLE, params)
                platform_url_id = self._register_platform_url(
                    url=url,
                    source="api",
                    entity_type="formulation_table",
                    metadata={"formulation": formulation, "formulation_id": fid},
                )

                yield Request(
                    url=url,
                    callback=self._parse_formulation_table,
                    headers={"Referer": SEARCH_URL},
                    meta={
                        "formulation": formulation,
                        "formulation_id": fid,
                        "_platform_url_id": platform_url_id,
                        "source": "api",
                        "entity_type": "formulation_table",
                    },
                    dont_filter=True,
                    errback=self._formulation_error,
                )

            return

    def _parse_formulation_table(self, response: TextResponse):
        """Step 4: Parse SKU list for a formulation, store MAIN rows, then fetch details."""
        formulation = response.meta["formulation"]
        table_data = safe_json(response)

        if table_data is None:
            retry_n = int(response.meta.get("json_retry", 0))
            if retry_n < MAX_RETRIES:
                self.logger.warning(
                    "[W%d] Non-JSON/empty response for '%s' (status=%s). Retrying %d/%d",
                    self.worker_id,
                    formulation,
                    response.status,
                    retry_n + 1,
                    MAX_RETRIES,
                )
                meta = dict(response.meta)
                meta["json_retry"] = retry_n + 1
                yield response.request.replace(dont_filter=True, meta=meta)
                return

            self.logger.error(
                "[W%d] Non-JSON/empty response for '%s' after retries (status=%s)",
                self.worker_id,
                formulation,
                response.status,
            )
            self._mark_formulation(formulation, "failed", error="Non-JSON/empty response")
            self.stats_errors += 1
            self._batch_pending_formulations -= 1
            if self._batch_pending_formulations <= 0:
                yield from self._claim_and_process()
            return

        if not isinstance(table_data, list):
            self.logger.error("[W%d] Unexpected response for '%s'", self.worker_id, formulation)
            self._mark_formulation(formulation, "failed", error="Unexpected response type")
            self.stats_errors += 1
            self._batch_pending_formulations -= 1
            if self._batch_pending_formulations <= 0:
                yield from self._claim_and_process()
            return

        if not table_data:
            self._mark_formulation(formulation, "zero_records")
            self.stats_zero += 1
            self._batch_pending_formulations -= 1
            if self._batch_pending_formulations <= 0:
                yield from self._claim_and_process()
            return

        # Truncate if needed
        max_rows = int(os.getenv("MAX_MEDICINES_PER_FORMULATION", "5000"))
        if len(table_data) > max_rows:
            self.logger.warning("[W%d] Truncating '%s' from %d to %d",
                                self.worker_id, formulation, len(table_data), max_rows)
            table_data = table_data[:max_rows]

        # Store MAIN SKU rows in DB
        sku_rows = []
        for row in table_data:
            if not isinstance(row, dict):
                continue
            hid = sget(row, "hiddenId").strip()
            if not hid:
                continue
            sku_rows.append({
                "run_id": self.run_id,
                "formulation": formulation,
                "hidden_id": hid,
                "sku_name": sget(row, "skuName"),
                "company": sget(row, "company"),
                "composition": sget(row, "composition"),
                "pack_size": sget(row, "packSize"),
                "dosage_form": sget(row, "dosageForm"),
                "schedule_status": sget(row, "scheduleStatus"),
                "ceiling_price": sget(row, "ceilingPrice"),
                "mrp": sget(row, "mrp"),
                "mrp_per_unit": sget(row, "mrpPerUnit"),
                "year_month": sget(row, "yearMonth"),
            })

        if sku_rows:
            upsert_items(self.db, "in_sku_main", sku_rows,
                         conflict_columns=["hidden_id", "run_id"])
            print(f"[DB] W{self.worker_id} | UPSERT | in_sku_main +{len(sku_rows)} rows ({formulation})", flush=True)
            self._upsert_platform_entities(sku_rows, response.meta.get("_platform_url_id"))

        # Count detail requests to track completion
        detail_count = 0
        detail_requests = []
        for row in table_data:
            if not isinstance(row, dict):
                continue
            hid = sget(row, "hiddenId").strip()
            if not hid:
                continue

            params_hid = {"hiddenId": hid}

            # skuMrpNew ? priority=1 ensures details run before next formulation table (priority=0)
            sku_mrp_url = build_api_url(API_SKU_MRP, params_hid)
            sku_mrp_url_id = self._register_platform_url(
                url=sku_mrp_url,
                source="api",
                entity_type="sku_mrp",
                metadata={"hidden_id": hid},
            )
            detail_requests.append(Request(
                url=sku_mrp_url,
                callback=self._parse_sku_mrp,
                errback=self._detail_error,
                headers={"Referer": SEARCH_URL},
                meta={"hidden_id": hid, "formulation": formulation, "api": "skuMrp",
                      "_platform_url_id": sku_mrp_url_id, "source": "api", "entity_type": "sku_mrp"},
                dont_filter=True,
                priority=1,
            ))

            # otherBrandPriceNew
            other_url = build_api_url(API_OTHER_BRANDS, params_hid)
            other_url_id = self._register_platform_url(
                url=other_url,
                source="api",
                entity_type="other_brands",
                metadata={"hidden_id": hid},
            )
            detail_requests.append(Request(
                url=other_url,
                callback=self._parse_other_brands,
                errback=self._detail_error,
                headers={"Referer": SEARCH_URL},
                meta={"hidden_id": hid, "formulation": formulation, "api": "otherBrands",
                      "_platform_url_id": other_url_id, "source": "api", "entity_type": "other_brands"},
                dont_filter=True,
                priority=1,
            ))

            # medDtlsNew
            med_url = build_api_url(API_MED_DTLS, params_hid)
            med_url_id = self._register_platform_url(
                url=med_url,
                source="api",
                entity_type="med_details",
                metadata={"hidden_id": hid},
            )
            detail_requests.append(Request(
                url=med_url,
                callback=self._parse_med_details,
                errback=self._detail_error,
                headers={"Referer": SEARCH_URL},
                meta={"hidden_id": hid, "formulation": formulation, "api": "medDtls",
                      "_platform_url_id": med_url_id, "source": "api", "entity_type": "med_details"},
                dont_filter=True,
                priority=1,
            ))
            detail_count += 3

        # Register pending details — formulation marked complete only when all finish
        self.stats_medicines += len(sku_rows)
        if detail_count > 0:
            self._pending_details[formulation] = {
                "pending": detail_count,
                "sku_count": len(sku_rows),
            }
            for req in detail_requests:
                yield req
        else:
            # No detail requests (no valid hidden_ids) — mark complete now
            self.stats_completed += 1
            self._mark_formulation(formulation, "completed", medicines=len(sku_rows))
            self._batch_pending_formulations -= 1
            # PERFORMANCE FIX: Log performance stats periodically
            self._log_performance_if_needed()
            if self._batch_pending_formulations <= 0:
                yield from self._claim_and_process()

    def _parse_sku_mrp(self, response: TextResponse):
        """Store skuMrpNew JSON payload."""
        hid = response.meta["hidden_id"]
        formulation = response.meta["formulation"]
        payload = safe_json(response)
        self.db.execute(
            "INSERT INTO in_sku_mrp (run_id, hidden_id, payload_json) VALUES (%s, %s, %s) "
            "ON CONFLICT DO NOTHING",
            (self.run_id, hid, json.dumps(payload, ensure_ascii=False)),
        )
        self.db.commit()
        self._upsert_platform_attributes(hid, {"sku_mrp": payload})
        if self._detail_done(formulation):
            yield from self._claim_and_process()

    def _parse_other_brands(self, response: TextResponse):
        """Store otherBrandPriceNew as individual brand_alternatives rows."""
        hid = response.meta["hidden_id"]
        payload = safe_json(response)

        if not isinstance(payload, list):
            payload = [payload] if isinstance(payload, dict) else []

        brand_rows = []
        for other in payload:
            if not isinstance(other, dict):
                continue
            brand_rows.append({
                "run_id": self.run_id,
                "hidden_id": hid,
                "brand_name": sget(other, "brandName"),
                "company": sget(other, "company"),
                "pack_size": sget(other, "packSize"),
                "brand_mrp": sget(other, "brandMrp"),
                "mrp_per_unit": sget(other, "mrpPerUnit"),
                "year_month": sget(other, "yearMonth"),
            })

        if brand_rows:
            from core.db.upsert import bulk_insert
            bulk_insert(self.db, "in_brand_alternatives", brand_rows)
            self.stats_substitutes += len(brand_rows)

        self._upsert_platform_attributes(hid, {"other_brands": payload})

        formulation = response.meta["formulation"]
        if self._detail_done(formulation):
            yield from self._claim_and_process()

    def _parse_med_details(self, response: TextResponse):
        """Store medDtlsNew JSON payload."""
        hid = response.meta["hidden_id"]
        formulation = response.meta["formulation"]
        payload = safe_json(response)
        self.db.execute(
            "INSERT INTO in_med_details (run_id, hidden_id, payload_json) VALUES (%s, %s, %s) "
            "ON CONFLICT DO NOTHING",
            (self.run_id, hid, json.dumps(payload, ensure_ascii=False)),
        )
        self.db.commit()
        self._upsert_platform_attributes(hid, {"med_details": payload})
        if self._detail_done(formulation):
            yield from self._claim_and_process()

    def _detail_done(self, formulation: str):
        """Decrement pending counter for a formulation; mark complete + claim next batch when all batch formulations done."""
        info = self._pending_details.get(formulation)
        if not info:
            return False  # already completed or not tracked
        info["pending"] -= 1
        if info["pending"] <= 0:
            self.stats_completed += 1
            self._mark_formulation(formulation, "completed", medicines=info["sku_count"])
            del self._pending_details[formulation]
            self._batch_pending_formulations -= 1
            if self._batch_pending_formulations <= 0:
                return True  # all formulations in batch done — claim next
        return False

    def _detail_error(self, failure):
        """Handle failure for detail API requests (skuMrp, otherBrands, medDtls)."""
        meta = failure.request.meta
        api = meta.get("api", "unknown")
        hid = meta.get("hidden_id", "unknown")
        formulation = meta.get("formulation", "unknown")
        self.logger.warning(
            "[W%d] Detail API '%s' failed for hid=%s formulation='%s': %s",
            self.worker_id, api, hid[:30], formulation, failure.value,
        )
        if self._detail_done(formulation):
            yield from self._claim_and_process()

    def _formulation_error(self, failure):
        """Handle request failure for a formulation."""
        formulation = failure.request.meta.get("formulation", "unknown")
        msg = str(failure.value)
        self.stats_errors += 1
        self._mark_formulation(formulation, "failed", error=msg)
        self.logger.error("[W%d] Formulation '%s' failed: %s", self.worker_id, formulation, msg)

        self._batch_pending_formulations -= 1
        if self._batch_pending_formulations <= 0:
            yield from self._claim_and_process()

    # ------------------------------------------------------------------
    # DB Helpers
    # ------------------------------------------------------------------

    def _init_db(self):
        """Open DB connection (schemas already applied by run_scrapy_india.py)."""
        self.db = PostgresDB("India")
        self.db.connect()
        self.logger.info("[W%d] DB connected to PostgreSQL, run_id=%s", self.worker_id, self.run_id)

    def _ensure_platform(self):
        if self._platform_ready:
            return
        try:
            from scripts.common.db import ensure_platform_schema
            ensure_platform_schema()
            self._platform_ready = True
        except Exception:
            self._platform_ready = False

    def _register_platform_url(self, url: str, source: Optional[str],
                               entity_type: Optional[str], metadata: Optional[Dict]):
        if not self._platform_ready:
            return None
        try:
            from scripts.common.db import upsert_url
            return upsert_url(url, self.country_name, source=source, entity_type=entity_type, metadata=metadata)
        except Exception:
            return None

    def _upsert_platform_entities(self, sku_rows: List[Dict[str, Any]], source_url_id: Optional[int]):
        if not self._platform_ready:
            return
        try:
            from scripts.common.db import insert_entity, insert_attribute
        except Exception:
            return

        for row in sku_rows:
            hid = row.get("hidden_id") or ""
            if not hid:
                continue
            try:
                entity_id = insert_entity(
                    entity_type="sku",
                    country=self.country_name,
                    source_url_id=source_url_id,
                    run_id=self.platform_run_id,
                    external_id=hid,
                    data=row,
                )
                self._entity_ids[hid] = entity_id
                for name, value in row.items():
                    if name == "run_id":
                        continue
                    insert_attribute(entity_id, name, value, source="scrape")
            except Exception:
                continue

    def _upsert_platform_attributes(self, hidden_id: str, attrs: Dict[str, Any]):
        if not self._platform_ready:
            return
        entity_id = self._entity_ids.get(hidden_id)
        if not entity_id:
            return
        try:
            from scripts.common.db import insert_attribute
            for name, value in attrs.items():
                insert_attribute(entity_id, name, value, source="scrape")
        except Exception:
            return

    def _claim_batch(self) -> List[str]:
        """Atomically claim a batch of pending formulations for this worker.

        Uses SELECT FOR UPDATE SKIP LOCKED to atomically transition rows
        from 'pending' to 'in_progress', preventing double-scraping.
        """
        # Use advisory lock or FOR UPDATE SKIP LOCKED for PostgreSQL
        # First, select and lock pending formulations
        cur = self.db.execute("""
            WITH claimed AS (
                SELECT formulation FROM in_formulation_status
                WHERE status = 'pending' AND run_id = %s
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE in_formulation_status fs
            SET status = 'in_progress', worker_id = %s,
                claimed_by = %s, claimed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            FROM claimed c
            WHERE fs.formulation = c.formulation AND fs.run_id = %s
            RETURNING fs.formulation
        """, (self.run_id, CLAIM_BATCH_SIZE, self.worker_id, self.worker_id, self.run_id))

        claimed = [row[0] for row in cur.fetchall()]
        self.db.commit()
        return claimed

    def _mark_formulation(self, formulation: str, status: str,
                          medicines: int = 0, substitutes: int = 0,
                          error: Optional[str] = None):
        """Update formulation_status table."""
        self.db.execute("""
            UPDATE in_formulation_status
            SET status = %s, medicines_count = %s, substitutes_count = %s,
                error_message = %s, attempts = attempts + 1, updated_at = CURRENT_TIMESTAMP
            WHERE formulation = %s AND run_id = %s
        """, (status, medicines, substitutes, error, formulation, self.run_id))
        self.db.commit()
        self._emit_progress(formulation, status, medicines)

    def _emit_progress(self, formulation: str, status: str, medicines: int):
        """Print [DB] activity line for GUI consumption.

        Note: [PROGRESS] lines are emitted by ProgressReporter in run_scrapy_india.py
        which aggregates all workers into a single status line.
        """
        if status == "zero_records":
            return  # Skip noisy zero-record lines
        tag = "OK" if status == "completed" else status.upper()
        med_info = f" ({medicines} medicines)" if medicines > 0 else ""
        print(f"[DB] W{self.worker_id} | {tag} | {formulation}{med_info} | sku_main={self.stats_medicines} brands={self.stats_substitutes}", flush=True)

    # ------------------------------------------------------------------
    # Spider close
    # ------------------------------------------------------------------

    def _log_performance_if_needed(self):
        """PERFORMANCE FIX: Log memory and performance stats periodically"""
        if self.stats_completed - self._last_perf_log >= self._perf_log_interval:
            self._last_perf_log = self.stats_completed
            try:
                import os
                import psutil
                proc = psutil.Process(os.getpid())
                mem_mb = proc.memory_info().rss / 1024 / 1024
                self.logger.info(
                    "[W%d] [PERFORMANCE] Memory: %.1fMB | Completed: %d | Medicines: %d | Errors: %d",
                    self.worker_id, mem_mb, self.stats_completed, 
                    self.stats_medicines, self.stats_errors
                )
            except Exception:
                pass  # psutil not available, skip silently

    def closed(self, reason):
        """Finalize: update run ledger, close DB."""
        if self.db:
            # Only mark run finished if this is the last worker
            # (run_ledger tracks the overall run, individual workers just log)
            self.db.close()

        self.logger.info(
            "[W%d] Spider closed: %s | medicines=%d, substitutes=%d, errors=%d, completed=%d, zero=%d",
            self.worker_id, reason, self.stats_medicines, self.stats_substitutes,
            self.stats_errors, self.stats_completed, self.stats_zero,
        )
