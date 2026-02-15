import os
import json
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("mock_step")

def main():
    log.info("Running mock step...")
    print("Mock step output to stdout")
    
    metrics_file = os.environ.get("PIPELINE_METRICS_FILE")
    if metrics_file:
        log.info(f"Checking metrics file: {metrics_file}")
        metrics = {
            "rows_processed": 123,
            "rows_inserted": 45,
            "custom_metric": "value"
        }
        with open(metrics_file, "w", encoding="utf-8") as f:
            json.dump(metrics, f)
        log.info(f"Wrote metrics: {metrics}")
    else:
        log.warning("No metrics file provided!")

if __name__ == "__main__":
    main()
