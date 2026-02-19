
# PreviouDayTender.py
# Flexible PNCP tender fetcher with multiple input modes:
#   - Date range filtering
#   - Title keyword filtering (or all tenders)
#   - Direct tender ID input
# Outputs CN numbers to database for GetData.py to process

from __future__ import annotations

import os
import sys
import time
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import requests

# Add repo root to path for core imports
_script_dir = Path(__file__).resolve().parent
_repo_root = _script_dir.parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from core.db.connection import CountryDB
from scripts.tender_brazil.db import BrazilRepository, apply_brazil_schema
from config_loader import load_env_file, getenv

# Configuration defaults
DEFAULT_DAYS = int(os.getenv("PNCP_DAYS", "7"))
DEFAULT_MODE = os.getenv("PNCP_MODE", "all").lower()  # "all", "search", "ids"
DEFAULT_SEARCH_FILE = os.getenv("PNCP_SEARCH_FILE", "SearchTerm.csv")
DEFAULT_ID_FILE = os.getenv("PNCP_ID_FILE", "TenderIDs.csv")
DEFAULT_AUTO_RUN = os.getenv("PNCP_AUTO_RUN", "true").lower() == "true"
DEFAULT_DATE_FROM = os.getenv("PNCP_DATE_FROM", "")  # YYYY-MM-DD
DEFAULT_DATE_TO = os.getenv("PNCP_DATE_TO", "")      # YYYY-MM-DD

BASE_URL = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
SEARCH_URL = "https://pncp.gov.br/api/search/"
BRAZIL_TZ = ZoneInfo("America/Sao_Paulo")

@dataclass
class FetchConfig:
    timeout_sec: int = 30
    max_retries: int = 5
    retry_backoff_sec: float = 1.5
    tamanho_pagina: int = 500
    sleep_between_pages_sec: float = 0.05

@dataclass
class TenderInfo:
    """Minimal tender info extracted from API."""
    cn_number: str
    numero_controle_pncp: str
    orgao_cnpj: str
    ano_compra: str
    sequencial_compra: str
    objeto: str
    orgao_nome: str
    data_publicacao: str
    modalidade: str

def parse_args() -> Tuple[int, str, str, str, bool, str, str]:
    """Parse command line arguments."""
    days = DEFAULT_DAYS
    mode = DEFAULT_MODE
    search_file = DEFAULT_SEARCH_FILE
    id_file = DEFAULT_ID_FILE
    auto_run = DEFAULT_AUTO_RUN
    date_from = DEFAULT_DATE_FROM
    date_to = DEFAULT_DATE_TO

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        arg = args[i]
        if arg in ("-d", "--days") and i + 1 < len(args):
            days = int(args[i + 1])
            i += 2
        elif arg in ("-m", "--mode") and i + 1 < len(args):
            mode = args[i + 1].lower()
            i += 2
        elif arg in ("-s", "--search") and i + 1 < len(args):
            search_file = args[i + 1]
            i += 2
        elif arg in ("--id-file") and i + 1 < len(args):
            id_file = args[i + 1]
            i += 2
        elif arg in ("-r", "--run"):
            auto_run = True
            i += 1
        elif arg in ("--no-run"):
            auto_run = False
            i += 1
        elif arg in ("--from") and i + 1 < len(args):
            date_from = args[i + 1]
            i += 2
        elif arg in ("--to") and i + 1 < len(args):
            date_to = args[i + 1]
            i += 2
        elif arg in ("-h", "--help"):
            print_usage()
            sys.exit(0)
        else:
            i += 1

    return days, mode, search_file, id_file, auto_run, date_from, date_to

def print_usage():
    print("""
Usage: python PreviouDayTender.py [options]

MODES:
  all     - Fetch ALL tenders in date range (no keyword filter)
  search  - Fetch tenders matching keywords from SearchTerm.csv
  ids     - Read tender IDs directly from TenderIDs.csv (or --id-file)

OPTIONS:
  -d, --days N          Number of days to fetch (default: 7)
  -m, --mode MODE       Mode: all/search/ids (default: all)
  -s, --search FILE     Search terms CSV file (default: SearchTerm.csv)
  --id-file FILE        Tender IDs file for 'ids' mode (default: TenderIDs.csv)
  --from DATE           Start date (YYYY-MM-DD, overrides --days)
  --to DATE             End date (YYYY-MM-DD, defaults to yesterday)
  -r, --run             Auto-run GetData.py (default: enabled)
  --no-run              Skip auto-run of GetData.py
  -h, --help            Show this help message
""")

def get_date_range(days: int, date_from: str = "", date_to: str = "") -> Tuple[str, str]:
    now_br = datetime.now(tz=BRAZIL_TZ)
    today_start = now_br.replace(hour=0, minute=0, second=0, microsecond=0)
    
    if date_from:
        start_dt = datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=BRAZIL_TZ)
    else:
        end_dt_default = today_start - timedelta(days=1)
        start_dt = end_dt_default - timedelta(days=days - 1)
    
    if date_to:
        end_dt = datetime.strptime(date_to, "%Y-%m-%d").replace(tzinfo=BRAZIL_TZ)
    else:
        end_dt = today_start - timedelta(days=1)
    
    return start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")

def read_search_terms(csv_path: str) -> List[str]:
    import csv
    if not os.path.exists(csv_path):
        return []

    search_terms = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            term = (row.get("Search term") or row.get("search_term") or row.get("keyword") 
                    or row.get("term") or row.get("palavra_chave") or "").strip()
            term = term.strip('"').strip("'").strip(",").strip()
            if term:
                search_terms.append(term.lower())
    return search_terms

def read_tender_ids(csv_path: str) -> List[TenderInfo]:
    import csv
    if not os.path.exists(csv_path):
        return []

    tenders = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cn = (row.get("CN Number") or row.get("cn_number") or row.get("numero_controle_pncp")
                    or row.get("CN") or row.get("cn") or row.get("tender_id") or "").strip()
            if cn:
                tenders.append(TenderInfo(
                    cn_number=cn,
                    numero_controle_pncp=cn,
                    orgao_cnpj=row.get("orgao_cnpj", ""),
                    ano_compra=row.get("ano", ""),
                    sequencial_compra=row.get("sequencial", ""),
                    objeto=row.get("objeto", ""),
                    orgao_nome=row.get("orgao", ""),
                    data_publicacao=row.get("data_publicacao", ""),
                    modalidade=row.get("modalidade", ""),
                ))
    return tenders

def request_with_retry(session, url, params, cfg):
    last_exc = None
    for attempt in range(1, cfg.max_retries + 1):
        try:
            resp = session.get(url, params=params, timeout=cfg.timeout_sec)
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(cfg.retry_backoff_sec * attempt)
                continue
            return resp
        except Exception as e:
            last_exc = e
            time.sleep(cfg.retry_backoff_sec * attempt)
    raise RuntimeError(f"Request failed. Last error: {last_exc!r}")

def extract_cn_number(tender):
    cn = tender.get("numeroControlePNCP") or tender.get("numero_controle_pncp")
    if cn: return str(cn).strip()
    orgao = tender.get("cnpj") or tender.get("cnpjOrgao") or tender.get("orgaoCnpj") or tender.get("orgao_cnpj")
    ano = tender.get("anoCompra") or tender.get("ano")
    seq = tender.get("sequencialCompra") or tender.get("numero_sequencial")
    if orgao and ano and seq: return f"{orgao}-1-{seq}/{ano}"
    return None

def extract_tender_info(tender):
    cn_number = extract_cn_number(tender)
    if not cn_number: return None
    return TenderInfo(
        cn_number=cn_number,
        numero_controle_pncp=tender.get("numeroControlePNCP") or tender.get("numero_controle_pncp", ""),
        orgao_cnpj=tender.get("cnpj") or tender.get("cnpjOrgao") or tender.get("orgaoCnpj") or tender.get("orgao_cnpj", ""),
        ano_compra=str(tender.get("anoCompra") or tender.get("ano", "")),
        sequencial_compra=str(tender.get("sequencialCompra") or tender.get("numero_sequencial", "")),
        objeto=tender.get("objetoCompra") or tender.get("objeto") or tender.get("title") or tender.get("description", ""),
        orgao_nome=tender.get("orgao") or tender.get("orgaoEntidade") or tender.get("razaoSocial") or tender.get("orgao_nome", ""),
        data_publicacao=tender.get("dataPublicacaoPncp") or tender.get("data_publicacao_pncp", ""),
        modalidade=tender.get("modalidadeNome") or tender.get("modalidade_licitacao_nome", ""),
    )

def matches_search_terms(tender, search_terms):
    if not search_terms: return True
    texto = " ".join([str(tender.get("objetoCompra", "")), str(tender.get("objeto", "")), str(tender.get("title", "")),
                  str(tender.get("description", "")), str(tender.get("orgaoEntidade", "")), str(tender.get("razaoSocial", ""))]).lower()
    return any(term in texto for term in search_terms)

def fetch_all_tenders(date_start, date_end, cfg, search_terms=None):
    session = requests.Session()
    all_tenders = []
    seen_cns = set()
    MODALITIES = [1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]

    for mod in MODALITIES:
        pagina = 1
        while True:
            params = {"dataInicial": date_start, "dataFinal": date_end, "codigoModalidadeContratacao": mod,
                      "pagina": pagina, "tamanhoPagina": min(cfg.tamanho_pagina, 50)}
            resp = request_with_retry(session, BASE_URL, params, cfg)
            if resp.status_code != 200: break
            data = resp.json()
            items = data.get("data", []) if isinstance(data, dict) else []
            if not items: break
            for it in items:
                if search_terms and not matches_search_terms(it, search_terms): continue
                tender_info = extract_tender_info(it)
                if tender_info and tender_info.cn_number not in seen_cns:
                    seen_cns.add(tender_info.cn_number)
                    all_tenders.append(tender_info)
            if pagina >= data.get("totalPaginas", 1): break
            pagina += 1
            time.sleep(cfg.sleep_between_pages_sec)
    return all_tenders

def fetch_search_tenders(date_start, date_end, search_terms, cfg):
    session = requests.Session()
    all_tenders = []
    seen_cns = set()
    date_start_dt = datetime.strptime(date_start, "%Y%m%d").replace(tzinfo=BRAZIL_TZ)
    date_end_dt = datetime.strptime(date_end, "%Y%m%d").replace(tzinfo=BRAZIL_TZ, hour=23, minute=59, second=59)

    for search_term in search_terms:
        pagina = 1
        while True:
            params = {"q": search_term, "tipos_documento": "edital", "ordenacao": "-data", 
                      "status": "todos", "pagina": pagina, "tam_pagina": cfg.tamanho_pagina}
            resp = request_with_retry(session, SEARCH_URL, params, cfg)
            if resp.status_code != 200: break
            data = resp.json()
            items = data.get("items", [])
            if not items: break
            stop_pagination = False
            for it in items:
                pub_date_str = it.get("data_publicacao_pncp", "")
                if pub_date_str:
                    try:
                        pub_date = datetime.fromisoformat(pub_date_str.replace("Z", "+00:00")).astimezone(BRAZIL_TZ)
                        if pub_date < date_start_dt:
                            stop_pagination = True
                            break
                        if not (date_start_dt <= pub_date <= date_end_dt): continue
                    except Exception as e:
                        print(f"  [WARN] Date parse failed for '{pub_date_str}': {e}", flush=True)
                tender_info = extract_tender_info(it)
                if tender_info and tender_info.cn_number not in seen_cns:
                    seen_cns.add(tender_info.cn_number)
                    all_tenders.append(tender_info)
            if stop_pagination or pagina * cfg.tamanho_pagina >= data.get("total", 0): break
            pagina += 1
            time.sleep(cfg.sleep_between_pages_sec)
    return all_tenders

def run_getdata():
    getdata_path = Path(__file__).parent / "GetData.py"
    try:
        subprocess.run([sys.executable, "-u", str(getdata_path)], check=True)
        return 0
    except subprocess.CalledProcessError as e:
        return e.returncode

def main() -> int:
    load_env_file()
    days, mode, search_file, id_file, auto_run, date_from, date_to = parse_args()

    run_id = os.environ.get("BRAZIL_RUN_ID")
    if not run_id:
        run_id = f"br_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        os.environ["BRAZIL_RUN_ID"] = run_id

    db = CountryDB("Tender_Brazil")
    apply_brazil_schema(db)
    repo = BrazilRepository(db, run_id)
    repo.start_run()
    
    print(f"[INFO] Collection Started (RunID: {run_id}, Mode: {mode})")

    if mode == "ids":
        tenders = read_tender_ids(str(Path(__file__).parent / id_file))
    else:
        date_start, date_end = get_date_range(days, date_from, date_to)
        cfg = FetchConfig()
        if mode == "search":
            search_terms = read_search_terms(search_file)
            tenders = fetch_search_tenders(date_start, date_end, search_terms, cfg) if search_terms else fetch_all_tenders(date_start, date_end, cfg)
        else:
            tenders = fetch_all_tenders(date_start, date_end, cfg)

    if not tenders:
        print("[WARN] No tenders found")
        db.close()
        return 0

    repo.insert_tender_list([t.cn_number for t in tenders])
    print(f"[OK] Saved {len(tenders)} tender IDs to database")
    db.close()

    if auto_run:
        return run_getdata()
    return 0

if __name__ == "__main__":
    main()
