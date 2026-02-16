import os

FILES_TO_DELETE = [
    r"d:\quad99\Scrappers\core\alerting_integration.py",
    r"d:\quad99\Scrappers\core\audit_logger.py",
    r"d:\quad99\Scrappers\core\base_scraper.py",
    r"d:\quad99\Scrappers\core\config_manager.py",
    r"d:\quad99\Scrappers\core\data_quality_checks.py",
    r"d:\quad99\Scrappers\core\logger.py",
    r"d:\quad99\Scrappers\core\memory_leak_detector.py",
    r"d:\quad99\Scrappers\core\pipeline_start_lock.py",
    r"d:\quad99\Scrappers\core\preflight_checks.py",
    r"d:\quad99\Scrappers\core\prometheus_exporter.py",
    r"d:\quad99\Scrappers\core\resource_monitor.py",
    r"d:\quad99\Scrappers\core\standalone_checkpoint.py",
    r"d:\quad99\Scrappers\core\step_hooks.py",
    r"d:\quad99\Scrappers\core\url_work_queue.py"
]

def delete_files():
    for file_path in FILES_TO_DELETE:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"Deleted {file_path}")
            else:
                print(f"File not found: {file_path}")
        except Exception as e:
            print(f"Error deleting {file_path}: {e}")

if __name__ == "__main__":
    delete_files()
