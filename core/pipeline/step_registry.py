"""
Centralized registry for pipeline steps.
Reduces ~200 lines of boilerplate in each country's pipeline runner.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Callable


@dataclass
class PipelineStep:
    number: int
    name: str
    script: str
    description: str
    is_optional: bool = False
    condition: Optional[Callable[[], bool]] = None


class StepRegistry:
    """Registry of steps for a specific scraper."""

    def __init__(self, scraper_id: str):
        self.scraper_id = scraper_id
        self.steps: List[PipelineStep] = []

    def add_step(self, number: int, name: str, script: str, description: str,
                 is_optional: bool = False,
                 condition: Optional[Callable[[], bool]] = None):
        """Add a step to the registry."""
        step = PipelineStep(number, name, script, description, is_optional, condition)
        self.steps.append(step)
        # Keep steps sorted by number
        self.steps.sort(key=lambda x: x.number)

    def get_step(self, number: int) -> Optional[PipelineStep]:
        """Get step by number."""
        for step in self.steps:
            if step.number == number:
                return step
        return None

    def get_active_steps(self) -> List[PipelineStep]:
        """Get steps that should run based on their conditions."""
        return [s for s in self.steps if s.condition is None or s.condition()]

    def get_max_step(self) -> int:
        """Get the highest step number."""
        if not self.steps:
            return 0
        return max(s.number for s in self.steps)


def get_argentina_steps() -> StepRegistry:
    """Standard steps for Argentina."""
    from scripts.Argentina.config_loader import USE_API_STEPS
    
    registry = StepRegistry("Argentina")
    registry.add_step(0, "Backup and Clean", "00_backup_and_clean.py", 
                      "Preparing: Backing up previous results and cleaning output directory")
    registry.add_step(1, "Get Product List", "01_getProdList.py", 
                      "Scraping: Fetching product list from AlfaBeta website")
    registry.add_step(2, "Prepare URLs", "02_prepare_urls.py", 
                      "Preparing: Building product URLs for scraping")
    registry.add_step(3, "Selenium Product Search", "03_alfabeta_selenium_scraper.py", 
                      "Scraping: Extracting product details using Selenium product search")
    registry.add_step(4, "Selenium Company Search", "03_alfabeta_selenium_company_scraper.py", 
                      "Scraping: Extracting remaining products using Selenium company search")
    registry.add_step(5, "API Scraper", "04_alfabeta_api_scraper.py", 
                      "Scraping: Extracting remaining products using API",
                      condition=lambda: USE_API_STEPS)
    registry.add_step(6, "Translate Using Dictionary", "05_translate_using_dict.py", 
                      "Processing: Translating Spanish terms to English using dictionary")
    registry.add_step(7, "Generate Output", "06_generate_output.py", 
                      "Generating: Creating final output files with PCID mapping")
    registry.add_step(8, "Scrape No-Data", "07_scrape_no_data.py", 
                      "Recovery: Retrying PCID no-data products using Selenium worker")
    registry.add_step(9, "Refresh Export", "08_refresh_export.py", 
                      "Refreshing: Re-running translation and output export after no-data retry")
    registry.add_step(10, "Statistics & Validation", "09_statistics_and_validation.py", 
                      "Validation: Computing detailed stats and data quality checks")
    return registry
