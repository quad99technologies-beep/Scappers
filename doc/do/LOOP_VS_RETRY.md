# Argentina Selenium: LOOP vs RETRY

Two separate concepts control how often we scrape a product:

---

## LOOP = full pass over the queue (how many times we “check”)

- **Meaning:** One **loop** = one entire run over all eligible products. After each loop, only products that **still have record count 0** are considered for the next loop.
- **Config:** `SELENIUM_MAX_LOOPS` (or legacy `SELENIUM_MAX_RUNS`). Default: 3.
- **Behaviour:**
  - **Loop 1:** Process all products with `total_records = 0` and `loop_count < max`. Some get data → `total_records > 0`; others stay 0 → `loop_count` incremented.
  - **Loop 2:** Process only products that **still have `total_records = 0`** (and `loop_count < max`). Again, some get data, some stay 0.
  - **Loop 3, …:** Same. After **max loops**, any product still with `total_records = 0` is sent to the API step (Step 4).
- **DB:** `ar_product_index.loop_count` = how many loops have **included** this product (incremented at the start of each attempt in a loop). Products with `total_records = 0` and `loop_count >= SELENIUM_MAX_LOOPS` are API-eligible.

**Summary:** *Loop = “how many full runs”; after each run we only re-check products whose record count is still 0.*

---

## RETRY = within one attempt (how many times to “try”)

- **Meaning:** For a **single** product in a **single** loop, if the page times out or extraction fails, we can **retry** a few times (reload, search again, etc.) before giving up for that loop.
- **Config:**
  - `MAX_RETRIES_TIMEOUT` – how many times to retry on page/timeout before marking for next loop or API (default: 2).
  - Inner “retry loop” (search → login check → retry) is capped (e.g. 5 iterations) when round-robin is off.
- **Behaviour:** One “attempt” = pick product → search → extract. If timeout: wait, then retry (same product, same loop). After `MAX_RETRIES_TIMEOUT` failures, we either requeue for the next loop (if `loop_count` still allows) or send to API.
- **Not the same as loop:** Retries do **not** increment `loop_count`. Only starting a product in a new **loop** (new full pass) does.

**Summary:** *Retry = “how many times to try within one attempt at a product” (e.g. on timeout or transient failure).*

---

## Quick reference

| Concept | Config (primary) | Legacy / other | Meaning |
|--------|-------------------|----------------|---------|
| **LOOP** | `SELENIUM_MAX_LOOPS` | `SELENIUM_MAX_RUNS`, `SELENIUM_ROUNDS` | Max number of **full passes**; after each pass only `total_records = 0` are checked again. |
| **RETRY** | `MAX_RETRIES_TIMEOUT` | Inner retry loops in worker | How many **tries per attempt** (e.g. on timeout) before giving up for this loop. |

- **API eligibility:** `total_records = 0` **and** `loop_count >= SELENIUM_MAX_LOOPS` → Step 4 (API).
- **Eligibility for next Selenium loop:** `total_records = 0` **and** `loop_count < SELENIUM_MAX_LOOPS`.
