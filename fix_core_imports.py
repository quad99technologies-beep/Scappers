import os

ROOT_DIR = r"d:\quad99\Scrappers\scripts"

MAPPINGS = {
    # Simple replacements
    "from core.logger": "from core.utils.logger",
    "import core.logger": "import core.utils.logger",
    "from core.config_manager": "from core.config.config_manager",
    "import core.config_manager": "import core.config.config_manager",
    "from core.audit_logger": "from core.monitoring.audit_logger",
    "import core.audit_logger": "import core.monitoring.audit_logger",
    "from core.alerting_integration": "from core.monitoring.alerting_integration",
    "import core.alerting_integration": "import core.monitoring.alerting_integration",
    "from core.resource_monitor": "from core.monitoring.resource_monitor",
    "import core.resource_monitor": "import core.monitoring.resource_monitor",
    "from core.memory_leak_detector": "from core.monitoring.memory_leak_detector",
    "import core.memory_leak_detector": "import core.monitoring.memory_leak_detector",
    "from core.prometheus_exporter": "from core.monitoring.prometheus_exporter",
    "import core.prometheus_exporter": "import core.monitoring.prometheus_exporter",
    "from core.data_quality_checks": "from core.data.data_quality_checks",
    "import core.data_quality_checks": "import core.data.data_quality_checks",
    "from core.pipeline_start_lock": "from core.pipeline.pipeline_start_lock",
    "import core.pipeline_start_lock": "import core.pipeline.pipeline_start_lock",
    "from core.preflight_checks": "from core.pipeline.preflight_checks",
    "import core.preflight_checks": "import core.pipeline.preflight_checks",
    "from core.standalone_checkpoint": "from core.pipeline.standalone_checkpoint",
    "import core.standalone_checkpoint": "import core.pipeline.standalone_checkpoint",
    "from core.step_hooks": "from core.pipeline.step_hooks",
    "import core.step_hooks": "import core.pipeline.step_hooks",
    "from core.url_work_queue": "from core.pipeline.url_work_queue",
    "import core.url_work_queue": "import core.pipeline.url_work_queue",
    "from core.base_scraper": "from core.pipeline.base_scraper",
    "import core.base_scraper": "import core.pipeline.base_scraper"
}

def fix_imports():
    count = 0
    for root, dirs, files in os.walk(ROOT_DIR):
        for file in files:
            if file.endswith(".py"):
                path = os.path.join(root, file)
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        content = f.read()
                    
                    new_content = content
                    for old, new in MAPPINGS.items():
                        new_content = new_content.replace(old, new)
                    
                    if new_content != content:
                        print(f"Fixing imports in {path}")
                        with open(path, "w", encoding="utf-8") as f:
                            f.write(new_content)
                        count += 1
                except Exception as e:
                    print(f"Skipping {path}: {e}")
    print(f"Fixed imports in {count} files.")

if __name__ == "__main__":
    fix_imports()
