"""
Scrapy pipelines for writing items into PostgreSQL DB.

PostgresPipeline:
- Opens a PostgresDB connection when spider starts
- Buffers items and flushes in batches
- Computes item_hash for deduplication
- Closes DB on spider close
"""

import json
import logging
from datetime import datetime, timezone

from core.db.postgres_connection import PostgresDB
from core.db.models import apply_common_schema, generate_run_id, run_ledger_start, run_ledger_finish
from core.db.upsert import bulk_insert, compute_item_hash

logger = logging.getLogger(__name__)

BATCH_SIZE = 100


class PostgresPipeline:
    """Write scraped items into PostgreSQL DB with batch buffering."""

    def __init__(self):
        self.db = None
        self.run_id = None
        self.buffer = []
        self.total_items = 0

    def open_spider(self, spider):
        """Open DB connection and start a run ledger entry."""
        country = getattr(spider, "country_name", spider.name)

        self.db = PostgresDB(country)
        self.db.connect()
        apply_common_schema(self.db)

        self.run_id = getattr(spider, "run_id", None) or generate_run_id()
        sql, params = run_ledger_start(self.run_id, country)
        self.db.execute(sql, params)
        self.db.commit()

        spider.run_id = self.run_id
        logger.info("PostgresPipeline opened: country=%s, run_id=%s",
                     country, self.run_id)

    def process_item(self, item, spider):
        """Buffer item, flush when buffer is full."""
        item_dict = dict(item)
        item_dict["run_id"] = self.run_id
        item_dict["scraped_at"] = datetime.now(timezone.utc).isoformat()

        # Compute hash excluding metadata fields
        hash_exclude = {"run_id", "source_url", "item_hash", "scraped_at"}
        hash_data = {k: v for k, v in item_dict.items() if k not in hash_exclude}
        item_dict["item_hash"] = compute_item_hash(hash_data)

        # Store as JSON in scraped_items table
        row = {
            "run_id": self.run_id,
            "source_url": item_dict.get("source_url", ""),
            "item_json": json.dumps(item_dict, default=str, ensure_ascii=False),
            "item_hash": item_dict["item_hash"],
        }
        self.buffer.append(row)

        if len(self.buffer) >= BATCH_SIZE:
            self._flush()

        return item

    def close_spider(self, spider):
        """Flush remaining items and close DB."""
        if self.buffer:
            self._flush()

        # Update run ledger
        sql, params = run_ledger_finish(
            self.run_id, "completed",
            items_scraped=self.total_items,
        )
        self.db.execute(sql, params)
        self.db.commit()
        self.db.close()
        logger.info("PostgresPipeline closed: %d items written, run_id=%s",
                     self.total_items, self.run_id)

    def _flush(self):
        """Write buffered items to DB."""
        if not self.buffer:
            return
        count = bulk_insert(self.db, "scraped_items", self.buffer)
        self.total_items += count
        self.buffer.clear()


# Alias for backward compatibility
SQLitePipeline = PostgresPipeline
