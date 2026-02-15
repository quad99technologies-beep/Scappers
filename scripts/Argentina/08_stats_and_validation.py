#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Argentina - Step 10: Statistics & Data Validation (DB-only)

Produces a detailed, run-scoped stats report covering:
- Queue / URL coverage (ar_product_index)
- Scrape coverage (ar_products, by source: selenium_product, selenium_company, api, step7)
- Scrape source tracking (which step scraped each product)
- Translation coverage (ar_products_translated)
- PCID mapping coverage (pcid_mapping + latest exports)
- OOS counts (PCID reference + exported OOS)
- "PCID but no data" and related gaps (from latest export reports)

Outputs:
- JSON report under output/Argentina/logs/
- Best-effort row logged to ar_export_reports as report_type='stats'
- Console output with comprehensive pipeline coverage report
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from config_loader import get_output_dir
from core.db.connection import CountryDB
from db.schema import apply_argentina_schema
from db.repositories import ArgentinaRepository


def _get_run_id(output_dir: Path) -> str:
    rid = os.environ.get("ARGENTINA_RUN_ID", "").strip()
    if rid:
        return rid
    run_id_file = output_dir / ".current_run_id"
    if run_id_file.exists():
        txt = run_id_file.read_text(encoding="utf-8").strip()
        if txt:
            os.environ["ARGENTINA_RUN_ID"] = txt
            return txt
    raise RuntimeError("ARGENTINA_RUN_ID not set and .current_run_id missing.")


def _fetch_one(cur, sql: str, params: tuple) -> Any:
    cur.execute(sql, params)
    return cur.fetchone()


def _latest_export_count(cur, run_id: str, report_type: str) -> int:
    cur.execute(
        """
        SELECT COALESCE(row_count, 0)
          FROM ar_export_reports
         WHERE run_id = %s AND report_type = %s
         ORDER BY created_at DESC
         LIMIT 1
        """,
        (run_id, report_type),
    )
    row = cur.fetchone()
    try:
        return int(row[0]) if row else 0
    except Exception:
        return 0


def _get_unscrapped_products(cur, run_id: str) -> List[Tuple[str, str]]:
    """Get list of products with total_records = 0."""
    cur.execute(
        """
        SELECT product, company
        FROM ar_product_index
        WHERE run_id = %s AND total_records = 0
        ORDER BY company, product
        """,
        (run_id,),
    )
    return [(row[0], row[1]) for row in cur.fetchall()]


def _get_loop_count_distribution(cur, run_id: str) -> Dict[int, int]:
    """Get distribution of loop counts."""
    cur.execute(
        """
        SELECT loop_count, COUNT(*) as cnt
        FROM ar_product_index
        WHERE run_id = %s
        GROUP BY loop_count
        ORDER BY loop_count
        """,
        (run_id,),
    )
    return {int(row[0]): int(row[1]) for row in cur.fetchall()}


def _get_error_summary(cur, run_id: str) -> Tuple[int, Dict[str, int]]:
    """Get error count and breakdown by type."""
    cur.execute(
        """
        SELECT COUNT(*) FROM ar_errors WHERE run_id = %s
        """,
        (run_id,),
    )
    total_errors = int((cur.fetchone() or [0])[0])
    
    if total_errors == 0:
        return total_errors, {}
    
    # Get error type breakdown (extract first part of error message)
    cur.execute(
        """
        SELECT error_type, COUNT(*) as cnt FROM (
            SELECT 
                CASE 
                    WHEN error_message LIKE 'Driver error during search%%' THEN 'Driver error during search'
                    WHEN error_message LIKE 'Driver error during extraction%%' THEN 'Driver error during extraction'
                    WHEN error_message ILIKE '%%timeout%%' THEN 'Navigation timeout'
                    WHEN error_message ILIKE '%%captcha%%' THEN 'Captcha detected'
                    ELSE 'Other'
                END as error_type
            FROM ar_errors
            WHERE run_id = %s
        ) sub
        GROUP BY error_type
        ORDER BY cnt DESC
        """,
        (run_id,),
    )
    error_types = {row[0]: int(row[1]) for row in cur.fetchall()}
    return total_errors, error_types



def main() -> None:
    output_dir = get_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    logs_dir = output_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    run_id = _get_run_id(output_dir)
    db = CountryDB("Argentina")
    apply_argentina_schema(db)
    repo = ArgentinaRepository(db, run_id)

    now = datetime.now()
    report: Dict[str, Any] = {
        "scraper": "Argentina",
        "run_id": run_id,
        "generated_at": now.isoformat(timespec="seconds"),
        "counts": {},
        "validation": {"warnings": [], "errors": []},
    }

    with db.cursor() as cur:
        # -----------------------------
        # PCID reference stats
        # -----------------------------
        cur.execute(
            """
            SELECT
              COUNT(*) AS total_rows,
              SUM(CASE WHEN COALESCE(NULLIF(TRIM(pcid),''),'') = '' THEN 1 ELSE 0 END) AS blank_pcid_rows,
              SUM(CASE WHEN UPPER(TRIM(COALESCE(pcid,''))) = 'OOS' THEN 1 ELSE 0 END) AS oos_rows,
              SUM(CASE WHEN COALESCE(NULLIF(TRIM(pcid),''),'') <> '' AND UPPER(TRIM(pcid)) <> 'OOS' THEN 1 ELSE 0 END) AS valid_rows,
              COUNT(DISTINCT CASE WHEN COALESCE(NULLIF(TRIM(pcid),''),'') <> '' AND UPPER(TRIM(pcid)) <> 'OOS' THEN TRIM(pcid) END) AS distinct_valid_pcid
            FROM pcid_mapping
            WHERE source_country = %s
            """,
            ("Argentina",),
        )
        pcid_ref = cur.fetchone() or (0, 0, 0, 0, 0)

        # -----------------------------
        # Queue / URL stats (run-scoped)
        # -----------------------------
        cur.execute(
            """
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN COALESCE(NULLIF(TRIM(url),''),'') <> '' THEN 1 ELSE 0 END) AS with_url,
              SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
              SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) AS in_progress,
              SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
              SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
              SUM(CASE WHEN COALESCE(total_records,0) > 0 THEN 1 ELSE 0 END) AS with_records,
              SUM(CASE WHEN COALESCE(total_records,0) = 0 THEN 1 ELSE 0 END) AS zero_records,
              SUM(CASE WHEN scraped_by_selenium THEN 1 ELSE 0 END) AS flagged_selenium,
              SUM(CASE WHEN scraped_by_api THEN 1 ELSE 0 END) AS flagged_api
            FROM ar_product_index
            WHERE run_id = %s
            """,
            (run_id,),
        )
        pi = cur.fetchone() or (0,) * 10

        # -----------------------------
        # Scrape stats (by source)
        # -----------------------------
        cur.execute(
            """
            SELECT
              COUNT(*) AS rows,
              COUNT(DISTINCT (input_company, input_product_name)) AS distinct_pairs,
              SUM(CASE WHEN source = 'selenium' THEN 1 ELSE 0 END) AS selenium_rows,
              SUM(CASE WHEN source = 'selenium_product' THEN 1 ELSE 0 END) AS selenium_product_rows,
              SUM(CASE WHEN source = 'selenium_company' THEN 1 ELSE 0 END) AS selenium_company_rows,
              SUM(CASE WHEN source = 'api' THEN 1 ELSE 0 END) AS api_rows,
              SUM(CASE WHEN source = 'step7' THEN 1 ELSE 0 END) AS step7_rows,
              SUM(CASE WHEN source = 'manual' THEN 1 ELSE 0 END) AS manual_rows
            FROM ar_products
            WHERE run_id = %s
            """,
            (run_id,),
        )
        prod = cur.fetchone() or (0, 0, 0, 0, 0, 0, 0, 0)

        # -----------------------------
        # Scrape source tracking (from ar_product_index)
        # -----------------------------
        cur.execute(
            """
            SELECT
              SUM(CASE WHEN scrape_source = 'selenium_product' THEN 1 ELSE 0 END) AS by_selenium_product,
              SUM(CASE WHEN scrape_source = 'selenium_company' THEN 1 ELSE 0 END) AS by_selenium_company,
              SUM(CASE WHEN scrape_source = 'api' THEN 1 ELSE 0 END) AS by_api,
              SUM(CASE WHEN scrape_source = 'step7' THEN 1 ELSE 0 END) AS by_step7,
              SUM(CASE WHEN scrape_source IS NULL AND total_records > 0 THEN 1 ELSE 0 END) AS by_unknown,
              SUM(CASE WHEN total_records = 0 THEN 1 ELSE 0 END) AS not_scraped
            FROM ar_product_index
            WHERE run_id = %s
            """,
            (run_id,),
        )
        scrape_src = cur.fetchone() or (0, 0, 0, 0, 0, 0)

        # Product-index coverage: how many queued pairs have at least one scraped row
        cur.execute(
            """
            SELECT
              COUNT(*) AS total_pairs,
              SUM(CASE WHEN p.exists_row = 1 THEN 1 ELSE 0 END) AS pairs_with_data
            FROM (
              SELECT pi.company, pi.product,
                     CASE WHEN EXISTS (
                       SELECT 1 FROM ar_products p
                        WHERE p.run_id = pi.run_id
                          AND p.input_company = pi.company
                          AND p.input_product_name = pi.product
                        LIMIT 1
                     ) THEN 1 ELSE 0 END AS exists_row
                FROM ar_product_index pi
               WHERE pi.run_id = %s
            ) p
            """,
            (run_id,),
        )
        cov = cur.fetchone() or (0, 0)

        # -----------------------------
        # Translation stats
        # -----------------------------
        cur.execute("SELECT COUNT(*) FROM ar_products_translated WHERE run_id = %s", (run_id,))
        translated_rows = int((cur.fetchone() or [0])[0])

        # -----------------------------
        # Export stats (latest per type)
        # -----------------------------
        export_counts = {
            "pcid_mapping_rows": _latest_export_count(cur, run_id, "pcid_mapping"),
            "pcid_missing_rows": _latest_export_count(cur, run_id, "pcid_missing"),
            "pcid_oos_rows": _latest_export_count(cur, run_id, "pcid_oos"),
            "pcid_no_data_rows": _latest_export_count(cur, run_id, "pcid_no_data"),
        }

        report["counts"].update(
            {
                "pcid_reference_total_rows": int(pcid_ref[0]),
                "pcid_reference_valid_rows": int(pcid_ref[3]),
                "pcid_reference_oos_rows": int(pcid_ref[2]),
                "pcid_reference_blank_pcid_rows": int(pcid_ref[1]),
                "pcid_reference_distinct_valid_pcid": int(pcid_ref[4]),
                "product_index_total": int(pi[0]),
                "product_index_with_url": int(pi[1]),
                "product_index_status_pending": int(pi[2]),
                "product_index_status_in_progress": int(pi[3]),
                "product_index_status_completed": int(pi[4]),
                "product_index_status_failed": int(pi[5]),
                "product_index_with_records": int(pi[6]),
                "product_index_zero_records": int(pi[7]),
                "product_index_flagged_selenium": int(pi[8]),
                "product_index_flagged_api": int(pi[9]),
                # Products rows by source (from ar_products.source column)
                "products_rows": int(prod[0]),
                "products_distinct_pairs": int(prod[1]),
                "products_rows_selenium": int(prod[2]),  # Legacy 'selenium' source
                "products_rows_selenium_product": int(prod[3]),  # Step 3: product search
                "products_rows_selenium_company": int(prod[4]),  # Step 4: company search
                "products_rows_api": int(prod[5]),  # Step 5: API
                "products_rows_step7": int(prod[6]),  # Step 8: no-data retry
                "products_rows_manual": int(prod[7]),  # Manual entries
                # Scrape source tracking (from ar_product_index.scrape_source column)
                "scraped_by_selenium_product": int(scrape_src[0]),  # Step 3
                "scraped_by_selenium_company": int(scrape_src[1]),  # Step 4
                "scraped_by_api": int(scrape_src[2]),  # Step 5
                "scraped_by_step7": int(scrape_src[3]),  # Step 8
                "scraped_by_unknown": int(scrape_src[4]),  # Has data but no source tracked
                "not_scraped": int(scrape_src[5]),  # total_records = 0
                "product_index_pairs_with_data": int(cov[1]),
                "product_index_pairs_without_data": int(cov[0]) - int(cov[1]),
                "translated_rows": int(translated_rows),
                **export_counts,
            }
        )

    # -----------------------------
    # Validation rules (warnings)
    # -----------------------------
    c = report["counts"]
    warnings = report["validation"]["warnings"]
    errors = report["validation"]["errors"]

    if c["product_index_total"] > 0 and c["product_index_with_url"] != c["product_index_total"]:
        warnings.append(
            f"URL coverage mismatch: product_index_with_url={c['product_index_with_url']} "
            f"vs product_index_total={c['product_index_total']} (Step 2 may be incomplete)."
        )

    if c["products_rows"] == 0:
        warnings.append("No rows in ar_products for this run_id (scrape may have failed or was skipped).")

    if c["translated_rows"] == 0 and c["products_rows"] > 0:
        warnings.append("No rows in ar_products_translated but ar_products has data (translation may have failed).")

    latest_export_total = (
        c["pcid_mapping_rows"] + c["pcid_missing_rows"] + c["pcid_oos_rows"]
    )
    if latest_export_total == 0 and c["products_rows"] > 0:
        warnings.append("Latest export report counts are zero (Step 6 may not have logged ar_export_reports).")

    # Derived indicators
    try:
        pcid_attach_total = max(latest_export_total, 0)
        pcid_mapped = int(c.get("pcid_mapping_rows", 0))
        report["counts"]["pcid_attach_total_rows"] = pcid_attach_total
        report["counts"]["pcid_attach_mapped_pct"] = round((pcid_mapped / pcid_attach_total) * 100, 2) if pcid_attach_total else 0.0
    except Exception:
        pass

    # Consistency check: translated rows should usually be <= products rows (one per product_id).
    if c["translated_rows"] > c["products_rows"] and c["products_rows"] > 0:
        warnings.append(
            f"Translated rows > products rows (translated_rows={c['translated_rows']} products_rows={c['products_rows']}). "
            "This is unexpected; investigate translation upsert logic."
        )

    # -----------------------------
    # Persist report to disk
    # -----------------------------
    ts = now.strftime("%Y%m%d_%H%M%S")
    out_path = logs_dir / f"argentina_stats_{run_id}_{ts}.json"
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    # Best-effort: log as an export report row for easy visibility in DB
    try:
        db2 = CountryDB("Argentina")
        apply_argentina_schema(db2)
        repo2 = ArgentinaRepository(db2, run_id)
        repo2.ensure_run_in_ledger(mode="resume")
        repo2.log_export_report("stats", str(out_path), None)
    except Exception:
        pass

    # =========================================================================
    # Fetch additional data for comprehensive report
    # =========================================================================
    with db.cursor() as cur:
        unscrapped_products = _get_unscrapped_products(cur, run_id)
        loop_distribution = _get_loop_count_distribution(cur, run_id)
        total_errors, error_types = _get_error_summary(cur, run_id)

    # Add to report
    report["unscrapped_products"] = [{"product": p, "company": co} for p, co in unscrapped_products]
    report["loop_distribution"] = loop_distribution
    report["error_summary"] = {"total": total_errors, "by_type": error_types}
    report["counts"]["total_from_website"] = c['product_index_total']
    
    # Re-save report with additional data
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    
    # =========================================================================
    # Print comprehensive pipeline coverage report
    # =========================================================================
    total_discovered = c['product_index_total']
    total_from_website = total_discovered
    total_scraped = c.get('scraped_by_selenium_product', 0) + c.get('scraped_by_selenium_company', 0) + c.get('scraped_by_api', 0) + c.get('scraped_by_step7', 0)
    unscrapped_count = c.get('not_scraped', 0)
    success_rate = (total_scraped / total_discovered * 100) if total_discovered > 0 else 0
    unscrapped_rate = (unscrapped_count / total_discovered * 100) if total_discovered > 0 else 0
    avg_records_per_product = (c['products_rows'] / total_scraped) if total_scraped > 0 else 0
    
    print("")
    print("=" * 80)
    print("           ARGENTINA PIPELINE DATA FLOW ANALYSIS")
    print("=" * 80)
    print(f"Run ID: {run_id}")
    print(f"Generated: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print("")
    
    # -------------------------------------------------------------------------
    # 1. Pipeline Summary
    # -------------------------------------------------------------------------
    print("-" * 80)
    print("1. PIPELINE COVERAGE SUMMARY")
    print("-" * 80)
    print(f"{'Metric':<45} {'Count':>10} {'Notes':<20}")
    print("-" * 80)
    print(f"{'Total from Website (Step 1)':<45} {total_from_website:>10} {'01_getProdList.py':<20}")
    print(f"{'  -> Scrape Queue':<45} {total_discovered:>10} {'ar_product_index':<20}")
    print("-" * 80)
    print(f"{'Scraped Successfully':<45} {total_scraped:>10} {'total_records > 0':<20}")
    print(f"{'Unscrapped':<45} {unscrapped_count:>10} {'total_records = 0':<20}")
    print("")
    
    # -------------------------------------------------------------------------
    # 2. Scraping Source Breakdown
    # -------------------------------------------------------------------------
    print("-" * 80)
    print("2. SCRAPING SOURCE BREAKDOWN")
    print("-" * 80)
    print(f"{'Source':<45} {'Count':>10} {'% of Scraped':>12}")
    print("-" * 80)
    
    selenium_product = c.get('scraped_by_selenium_product', 0)
    selenium_company = c.get('scraped_by_selenium_company', 0)
    api_count = c.get('scraped_by_api', 0)
    step7_count = c.get('scraped_by_step7', 0)
    unknown_count = c.get('scraped_by_unknown', 0)
    
    def pct(val, total):
        return f"{(val/total*100):.1f}%" if total > 0 else "0.0%"
    
    print(f"{'selenium_product (Product Search - Step 3)':<45} {selenium_product:>10} {pct(selenium_product, total_scraped):>12}")
    print(f"{'selenium_company (Company Search - Step 4)':<45} {selenium_company:>10} {pct(selenium_company, total_scraped):>12}")
    print(f"{'api (API Scrape - Step 5)':<45} {api_count:>10} {pct(api_count, total_scraped):>12}")
    print(f"{'step7 (No-Data Retry - Step 7)':<45} {step7_count:>10} {pct(step7_count, total_scraped):>12}")
    if unknown_count > 0:
        print(f"{'unknown (Has data, no source tracked)':<45} {unknown_count:>10} {pct(unknown_count, total_scraped):>12}")
    print("-" * 80)
    print(f"{'TOTAL SCRAPED':<45} {total_scraped:>10} {'100.0%':>12}")
    print("")
    
    # -------------------------------------------------------------------------
    # 3. Unscrapped Products
    # -------------------------------------------------------------------------
    if unscrapped_products:
        print("-" * 80)
        print(f"3. UNSCRAPPED PRODUCTS ({len(unscrapped_products)} total)")
        print("-" * 80)
        print(f"{'Product':<40} {'Company':<35}")
        print("-" * 80)
        for product, company in unscrapped_products[:20]:  # Limit to 20 for display
            print(f"{product[:39]:<40} {company[:34]:<35}")
        if len(unscrapped_products) > 20:
            print(f"... and {len(unscrapped_products) - 20} more")
        print("")
    
    # -------------------------------------------------------------------------
    # 4. Output Records
    # -------------------------------------------------------------------------
    print("-" * 80)
    print("4. OUTPUT RECORDS")
    print("-" * 80)
    print(f"{'Table/Report':<45} {'Records':>10}")
    print("-" * 80)
    print(f"{'ar_products (raw presentations)':<45} {c['products_rows']:>10}")
    print(f"{'ar_products_translated':<45} {c['translated_rows']:>10}")
    print(f"{'Unique input products in ar_products':<45} {c['products_distinct_pairs']:>10}")
    print("")
    print("Export Files:")
    print(f"  {'pcid_mapping.csv':<43} {c['pcid_mapping_rows']:>10}")
    print(f"  {'pcid_missing.csv':<43} {c['pcid_missing_rows']:>10}")
    print(f"  {'pcid_oos.csv':<43} {c['pcid_oos_rows']:>10}")
    print(f"  {'pcid_no_data.csv':<43} {c['pcid_no_data_rows']:>10}")
    print("")
    
    # -------------------------------------------------------------------------
    # 5. Loop Count Distribution
    # -------------------------------------------------------------------------
    if loop_distribution:
        print("-" * 80)
        print("5. LOOP COUNT DISTRIBUTION")
        print("-" * 80)
        print(f"{'Loop':<20} {'Products':>10}")
        print("-" * 80)
        for loop, count in sorted(loop_distribution.items()):
            print(f"{'Loop ' + str(loop):<20} {count:>10}")
        print("")
    
    # -------------------------------------------------------------------------
    # 6. Errors Summary
    # -------------------------------------------------------------------------
    if total_errors > 0:
        print("-" * 80)
        print(f"6. ERRORS SUMMARY ({total_errors} total)")
        print("-" * 80)
        print(f"{'Error Type':<50} {'Count':>10} {'%':>10}")
        print("-" * 80)
        for err_type, count in error_types.items():
            print(f"{err_type:<50} {count:>10} {pct(count, total_errors):>10}")
        print("")
    
    # -------------------------------------------------------------------------
    # 7. Success Rate Summary
    # -------------------------------------------------------------------------
    print("-" * 80)
    print("7. SUCCESS RATE SUMMARY")
    print("-" * 80)
    print(f"{'Metric':<45} {'Value':>15}")
    print("-" * 80)
    print(f"{'Scraping Success Rate':<45} {f'{success_rate:.2f}% ({total_scraped}/{total_discovered})':>15}")
    print(f"{'Unscrapped Rate':<45} {f'{unscrapped_rate:.2f}% ({unscrapped_count}/{total_discovered})':>15}")
    print(f"{'Avg Records per Product':<45} {f'{avg_records_per_product:.2f}':>15}")
    print("")
    
    # -------------------------------------------------------------------------
    # 8. Pipeline Flow Diagram
    # -------------------------------------------------------------------------
    print("-" * 80)
    print("8. PIPELINE FLOW DIAGRAM")
    print("-" * 80)
    print("""
    Step 1: Get Product List (01_getProdList.py)
         |
         v
    Website Products ---> ar_product_index: {total} products
                  |
                  v
             Step 2: Prepare URLs
                  |
                  v
             Step 3: Selenium Product Search ---> {sel_prod} products
                  |
                  v
             Step 4: Selenium Company Search ---> {sel_comp} products (fallback)
                  |
                  v
             Step 5: API Scrape ---> {api} products
                  |
                  v
             Step 7: No-Data Retry ---> {step7} products
                  |
                  +---> {unscrapped} products unscrapped
                  |
                  v
             ar_products: {prod_rows} presentation records
                  |
                  +---> Step 6: Translate ---> ar_products_translated: {trans}
                  |
                  +---> Step 8: Generate Output
                           |
                           +---> pcid_mapping.csv: {pcid_map} rows
                           +---> pcid_missing.csv: {pcid_miss} rows
                           +---> pcid_oos.csv: {pcid_oos} rows
                           +---> pcid_no_data.csv: {pcid_nodata} rows
""".format(
        total=total_discovered,
        sel_prod=selenium_product,
        sel_comp=selenium_company,
        api=api_count,
        step7=step7_count,
        unscrapped=unscrapped_count,
        prod_rows=c['products_rows'],
        trans=c['translated_rows'],
        pcid_map=c['pcid_mapping_rows'],
        pcid_miss=c['pcid_missing_rows'],
        pcid_oos=c['pcid_oos_rows'],
        pcid_nodata=c['pcid_no_data_rows'],
    ))
    
    # -------------------------------------------------------------------------
    # Warnings and Errors
    # -------------------------------------------------------------------------
    if warnings:
        print("-" * 80)
        print("WARNINGS")
        print("-" * 80)
        for w in warnings:
            print(f"  - {w}")
        print("")
    
    if errors:
        print("-" * 80)
        print("ERRORS")
        print("-" * 80)
        for e in errors:
            print(f"  - {e}")
        print("")
        raise SystemExit(1)
    
    print("=" * 80)
    print(f"Report saved to: {out_path}")
    print("=" * 80)


if __name__ == "__main__":
    main()

