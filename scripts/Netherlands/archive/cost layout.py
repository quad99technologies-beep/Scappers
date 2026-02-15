# nl_cost_html_to_csv_flush100.py
# Output CSV with 2 columns: url, cost_html (raw <form><table class="cost-table">...</table></form>)
# Flushes every 100 URLs

import json, csv, time
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Error as PWError

URLS_JSON = r"D:\quad99\Scrappers\scripts\Netherlands\archive\output_nl_cost_per_day_only\all_detail_urls.json"
OUT_CSV = r"D:\quad99\Scrappers\scripts\Netherlands\archive\url_cost_html.csv"

HEADLESS = True
TIMEOUT_MS = 60_000
FLUSH_EVERY = 100

NETWORK_RETRY_MAX = 3
NETWORK_RETRY_DELAY = 2  # seconds


def goto_with_retry(page, url: str) -> None:
    last_err = None
    for attempt in range(1, NETWORK_RETRY_MAX + 1):
        try:
            page.goto(url, wait_until="domcontentloaded")
            return
        except (PWTimeoutError, PWError) as e:
            last_err = e
            if attempt < NETWORK_RETRY_MAX:
                time.sleep(NETWORK_RETRY_DELAY * attempt)
            else:
                raise last_err


def expand_kosten(page) -> None:
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(700)
    page.evaluate("""
      () => {
        const hs = document.querySelectorAll('h2.page-module-title');
        for (const h of hs) {
          const t = (h.textContent || '').trim();
          if (t.includes('Kosten')) {
            h.scrollIntoView({behavior:'instant', block:'center'});
            if (h.classList.contains('collapsible-closed')) h.click();
            return true;
          }
        }
        return false;
      }
    """)
    page.wait_for_timeout(1200)


def extract_cost_html(page) -> str | None:
    forms = page.locator("form:has(table.cost-table)")
    if forms.count() == 0:
        # fallback selector
        forms = page.locator("form:has(table[class*='cost'])")
    if forms.count() == 0:
        return None

    # If multiple forms exist, concatenate them
    html_blocks = []
    for i in range(forms.count()):
        html_blocks.append(forms.nth(i).evaluate("el => el.outerHTML"))

    return "\n\n".join(html_blocks)


def main():
    urls = json.loads(Path(URLS_JSON).read_text(encoding="utf-8"))
    print(f"[INFO] URLs loaded: {len(urls)}")

    # Write header once
    write_header = not Path(OUT_CSV).exists()
    out_f = open(OUT_CSV, "a", encoding="utf-8", newline="")
    writer = csv.writer(out_f)
    if write_header:
        writer.writerow(["url", "cost_html"])

    buffer_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        ctx = browser.new_context()
        page = ctx.new_page()
        page.set_default_timeout(TIMEOUT_MS)

        for i, url in enumerate(urls, start=1):
            try:
                goto_with_retry(page, url)
                expand_kosten(page)
                cost_html = extract_cost_html(page)
            except PWTimeoutError:
                cost_html = "TIMEOUT"
            except Exception as e:
                cost_html = f"ERROR: {type(e).__name__}: {e}"

            buffer_rows.append([url, cost_html or "NO_COST_TABLE"])

            if i % FLUSH_EVERY == 0:
                writer.writerows(buffer_rows)
                out_f.flush()
                buffer_rows.clear()
                print(f"[FLUSH] {i}/{len(urls)} rows written")

        # final flush
        if buffer_rows:
            writer.writerows(buffer_rows)
            out_f.flush()
            buffer_rows.clear()
            print("[FLUSH] Final batch written")

        browser.close()
        out_f.close()

    print("[DONE] CSV written:", OUT_CSV)


if __name__ == "__main__":
    main()
