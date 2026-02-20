
import os
import sys

sys.path.insert(0, "d:/quad99/Scrappers")

from core.db.connection import CountryDB

run_id = os.environ.get("ITALY_RUN_ID", "").strip()
if not run_id:
    raise SystemExit("Set ITALY_RUN_ID to inspect progress for a specific run.")

db = CountryDB("Italy")
with db.cursor() as cur:
    cur.execute(
        """
        SELECT SPLIT_PART(progress_key, ':', 1) AS keyword, status, COUNT(*) AS cnt
        FROM it_step_progress
        WHERE run_id = %s AND step_number = 1
        GROUP BY keyword, status
        ORDER BY keyword, status
        """,
        (run_id,),
    )
    rows = cur.fetchall()
    print(f"Step 1 progress for run_id={run_id}")
    for keyword, status, cnt in rows:
        print(f"  {keyword:10} {status:12} {cnt}")

    cur.execute(
        """
        SELECT keyword, metric_name, metric_value
        FROM it_run_stats
        WHERE run_id = %s AND step_number = 1
        ORDER BY keyword, metric_name
        """,
        (run_id,),
    )
    stats = cur.fetchall()
    if stats:
        print("\nStep 1 stats (it_run_stats):")
        for keyword, metric, value in stats:
            print(f"  {keyword:10} {metric:18} {value}")

    cur.execute(
        """
        SELECT progress_key, status
        FROM it_step_progress
        WHERE run_id = %s AND step_number = 1
        ORDER BY id DESC
        LIMIT 10
        """,
        (run_id,),
    )
    print("\nLatest 10 windows:")
    for progress_key, status in cur.fetchall():
        print(f"  {progress_key}: {status}")
