from prefect import flow, task, pause_flow_run, get_run_logger
import subprocess, os
from pathlib import Path

PROJECT_DIR = Path(__file__).parent

SCRIPTS = [
    "1. getCompanyList.py",   # adjust names exactly as on disk
    # "2. getProdList.py",
    # "3. alfabeta_scraper_labs.py",
    # "4. TranslateUsingDictionary.py",
    # "5. Generate Output.py",
]

def sanitize(s: str) -> str:
    return s.encode("ascii", "replace").decode("ascii")

@task
def run_script(script_name: str):
    p = PROJECT_DIR / script_name
    if not p.exists():
        avail = "\n".join(sorted(x.name for x in PROJECT_DIR.glob("*.py")))
        raise FileNotFoundError(f"Script not found: {p}\nAvailable .py files:\n{avail}")
    r = subprocess.run(
        ["python", str(p)],
        cwd=str(PROJECT_DIR),
        text=True,
        capture_output=True,
        env={**os.environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"},
    )
    if r.stdout: print(sanitize(r.stdout))
    if r.returncode != 0:
        raise RuntimeError(f"{script_name} failed ({r.returncode}).\n{sanitize(r.stderr or '')}")
    print(f"[OK] {script_name} completed.")
    return True

@flow(name="Alfabeta Scraper End-to-End (UI Approval)")
def alfabeta_pipeline():
    # STEP 1
    for s in SCRIPTS[:3]:
        run_script(s)

    # Pause with a message in logs (no args to pause_flow_run in Prefect 3)
    logger = get_run_logger()
    logger.info("Step 1 done. Review outputs. Click 'Resume' in the UI to start Step 2.")
    pause_flow_run()

    # STEP 2
    for s in SCRIPTS[3:]:
        run_script(s)
