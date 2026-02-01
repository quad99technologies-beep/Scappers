/**
 * SQLite store for writing scraped items from Crawlee (Node.js) into the
 * country SQLite DB. Uses better-sqlite3 for synchronous, WAL-mode writes.
 *
 * Writes to the same DB schema as Python's core/db/ layer, so both sides
 * can read/write the same DB.
 *
 * Usage:
 *   const store = new SQLiteStore('output/Malaysia/malaysia.db', 'run_20260127_abc');
 *   store.insertItem('products', { registration_no: '...', product_name: '...' });
 *   store.logRequest('https://...', 200, 350);
 *   store.close();
 */

import Database from 'better-sqlite3';
import * as crypto from 'crypto';

export class SQLiteStore {
    private db: Database.Database;
    private runId: string;
    private insertItemStmt: Database.Statement | null = null;
    private insertRequestStmt: Database.Statement;

    constructor(dbPath: string, runId: string) {
        this.db = new Database(dbPath);
        this.runId = runId;

        // Set WAL mode and pragmas (matching Python's CountryDB)
        this.db.pragma('journal_mode = WAL');
        this.db.pragma('busy_timeout = 5000');
        this.db.pragma('foreign_keys = ON');
        this.db.pragma('synchronous = NORMAL');
        this.db.pragma('cache_size = -64000');
        this.db.pragma('temp_store = MEMORY');

        // Ensure common tables exist
        this.db.exec(`
            CREATE TABLE IF NOT EXISTS run_ledger (
                run_id TEXT PRIMARY KEY,
                scraper_name TEXT NOT NULL,
                started_at TEXT NOT NULL DEFAULT (datetime('now')),
                ended_at TEXT,
                status TEXT NOT NULL DEFAULT 'running',
                step_count INTEGER DEFAULT 0,
                items_scraped INTEGER DEFAULT 0,
                items_exported INTEGER DEFAULT 0,
                error_message TEXT,
                git_commit TEXT,
                config_hash TEXT,
                metadata_json TEXT
            );
            CREATE TABLE IF NOT EXISTS http_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
                url TEXT NOT NULL,
                method TEXT DEFAULT 'GET',
                status_code INTEGER,
                response_bytes INTEGER,
                elapsed_ms REAL,
                error TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS scraped_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
                source_url TEXT,
                item_json TEXT NOT NULL,
                item_hash TEXT,
                parse_status TEXT DEFAULT 'ok',
                error_reason TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_items_run ON scraped_items(run_id);
            CREATE INDEX IF NOT EXISTS idx_items_hash ON scraped_items(item_hash);
            CREATE INDEX IF NOT EXISTS idx_req_run ON http_requests(run_id);
        `);

        // Prepare statements
        this.insertRequestStmt = this.db.prepare(`
            INSERT INTO http_requests (run_id, url, method, status_code, elapsed_ms, error)
            VALUES (?, ?, ?, ?, ?, ?)
        `);
    }

    /**
     * Start a run by inserting into run_ledger.
     */
    startRun(scraperName: string): void {
        this.db.prepare(`
            INSERT OR IGNORE INTO run_ledger (run_id, scraper_name, status)
            VALUES (?, ?, 'running')
        `).run(this.runId, scraperName);
    }

    /**
     * Insert a scraped item into the scraped_items table as JSON.
     */
    insertItem(table: string, item: Record<string, unknown>): void {
        const json = JSON.stringify(item);
        const hash = this.computeHash(item);
        this.db.prepare(`
            INSERT INTO scraped_items (run_id, source_url, item_json, item_hash)
            VALUES (?, ?, ?, ?)
        `).run(this.runId, (item.source_url as string) || '', json, hash);
    }

    /**
     * Insert multiple items in a single transaction (batch).
     */
    insertBatch(table: string, items: Record<string, unknown>[]): void {
        const insert = this.db.prepare(`
            INSERT INTO scraped_items (run_id, source_url, item_json, item_hash)
            VALUES (?, ?, ?, ?)
        `);
        const tx = this.db.transaction((rows: Record<string, unknown>[]) => {
            for (const item of rows) {
                const json = JSON.stringify(item);
                const hash = this.computeHash(item);
                insert.run(this.runId, (item.source_url as string) || '', json, hash);
            }
        });
        tx(items);
    }

    /**
     * Insert into a custom table (for country-specific schemas).
     * Columns are derived from the item keys.
     */
    insertCustom(table: string, item: Record<string, unknown>): void {
        const keys = Object.keys(item);
        const cols = keys.join(', ');
        const placeholders = keys.map(() => '?').join(', ');
        const values = keys.map((k) => item[k]);
        this.db.prepare(`INSERT INTO ${table} (${cols}) VALUES (${placeholders})`).run(...values);
    }

    /**
     * Log an HTTP request.
     */
    logRequest(
        url: string,
        statusCode: number,
        elapsedMs: number,
        method: string = 'GET',
        error: string | null = null,
    ): void {
        this.insertRequestStmt.run(this.runId, url, method, statusCode, elapsedMs, error);
    }

    /**
     * Finish the run by updating run_ledger.
     */
    finishRun(status: string, itemsScraped: number, error: string | null = null): void {
        this.db.prepare(`
            UPDATE run_ledger
            SET ended_at = datetime('now'), status = ?, items_scraped = ?, error_message = ?
            WHERE run_id = ?
        `).run(status, itemsScraped, error, this.runId);
    }

    /**
     * Close the database connection.
     */
    close(): void {
        this.db.close();
    }

    /**
     * Compute SHA-256 hash of item (excluding metadata keys).
     */
    private computeHash(item: Record<string, unknown>): string {
        const exclude = new Set(['run_id', 'source_url', 'item_hash', 'scraped_at']);
        const filtered: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(item).sort()) {
            if (!exclude.has(k)) filtered[k] = v;
        }
        return crypto.createHash('sha256').update(JSON.stringify(filtered)).digest('hex');
    }
}
