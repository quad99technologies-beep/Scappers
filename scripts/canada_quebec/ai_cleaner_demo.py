import sys
from pathlib import Path

# Add repo root to path for core imports (MUST be before any core imports)
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.ai import ai_service

def run_generic_demo():
    print("=== Generic AI Cleaner Service Demo ===")
    
    # 1. QUEBEC EXAMPLE (PDF extraction)
    quebec_data = [
        "02312445  TEVA-VENLAFAXINE XR  TEVA  37.5 MG  CAP (XR)  0.1542",
        "ABACAVIR (SULFATE D') / LAMIVUDINE 600 mg / 300 mg CP 30 156.0000 5.2000 02396762"
    ]
    
    # 2. ARGENTINA EXAMPLE (Website extraction)
    argentina_data = [
        "IBUPROFENO 400 MG TABLETAS RECUBIERTAS X 20 LABORATORIO ROEMMERS ARS 4500.50",
    ]

    if not ai_service.is_enabled:
        print("\n[!] AI Service is disabled (Check GOOGLE_API_KEY).")
        return

    print("\n--- Processing Quebec Data ---")
    quebec_results = ai_service.clean_pharmaceutical_data(
        quebec_data, 
        country_context="Canada Quebec PDF"
    )
    print(json.dumps(quebec_results, indent=2))

    print("\n--- Processing Argentina Data ---")
    argentina_results = ai_service.clean_pharmaceutical_data(
        argentina_data, 
        country_context="Argentina Alfabeta Scraper"
    )
    print(json.dumps(argentina_results, indent=2))

if __name__ == "__main__":
    run_generic_demo()

if __name__ == "__main__":
    run_quebec_demo()
