# PreviouDayTender.py
# Flexible PNCP tender fetcher with multiple input modes:
#   - Date range filtering
#   - Title keyword filtering (or all tenders)
#   - Direct tender ID input
# Outputs CN numbers to Input.csv for GetData.py to process

from __future__ import annotations

import csv
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import requests

# Configuration defaults
DEFAULT_DAYS = int(os.getenv("PNCP_DAYS", "7"))
DEFAULT_MODE = os.getenv("PNCP_MODE", "all").lower()  # "all", "search", "ids"
DEFAULT_SEARCH_FILE = os.getenv("PNCP_SEARCH_FILE", "SearchTerm.csv")
DEFAULT_INPUT_CSV = os.getenv("PNCP_INPUT_CSV", "Input.csv")
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


def parse_args() -> Tuple[int, str, str, str, str, bool, str, str]:
    """Parse command line arguments."""
    days = DEFAULT_DAYS
    mode = DEFAULT_MODE
    search_file = DEFAULT_SEARCH_FILE
    input_csv = DEFAULT_INPUT_CSV
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
        elif arg in ("-i", "--input") and i + 1 < len(args):
            input_csv = args[i + 1]
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
        elif not arg.startswith("-") and input_csv == DEFAULT_INPUT_CSV:
            input_csv = arg
            i += 1
        else:
            i += 1

    return days, mode, search_file, input_csv, id_file, auto_run, date_from, date_to


def print_usage():
    print("""
Usage: python PreviouDayTender.py [options] [input_csv]

MODES:
  all     - Fetch ALL tenders in date range (no keyword filter)
  search  - Fetch tenders matching keywords from SearchTerm.csv
  ids     - Read tender IDs directly from TenderIDs.csv (or --id-file)

OPTIONS:
  -d, --days N          Number of days to fetch (default: 7)
  -m, --mode MODE       Mode: all/search/ids (default: all)
  -s, --search FILE     Search terms CSV file (default: SearchTerm.csv)
  --id-file FILE        Tender IDs file for 'ids' mode (default: TenderIDs.csv)
  -i, --input FILE      Output Input.csv file for GetData.py (default: Input.csv)
  --from DATE           Start date (YYYY-MM-DD, overrides --days)
  --to DATE             End date (YYYY-MM-DD, defaults to yesterday)
  -r, --run             Auto-run GetData.py (default: enabled)
  --no-run              Skip auto-run of GetData.py
  -h, --help            Show this help message

ENVIRONMENT VARIABLES:
  PNCP_DAYS, PNCP_MODE, PNCP_SEARCH_FILE, PNCP_INPUT_CSV
  PNCP_ID_FILE, PNCP_AUTO_RUN, PNCP_DATE_FROM, PNCP_DATE_TO

EXAMPLES:
  # Fetch ALL tenders from last 7 days
  python PreviouDayTender.py

  # Fetch last 3 days, auto-run GetData.py
  python PreviouDayTender.py -d 3 -r

  # Fetch tenders matching search terms
  python PreviouDayTender.py -m search -d 7

  # Fetch with custom date range
  python PreviouDayTender.py --from 2025-01-01 --to 2025-01-31

  # Use specific tender IDs from file
  python PreviouDayTender.py -m ids --id-file MyTenders.csv -r

  # Custom search terms file
  python PreviouDayTender.py -m search -s MyKeywords.csv -d 10 -r
""")


def get_date_range(days: int, date_from: str = "", date_to: str = "") -> Tuple[str, str]:
    """Get date range for fetching tenders."""
    now_br = datetime.now(tz=BRAZIL_TZ)
    today_start = now_br.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Parse custom dates if provided
    if date_from:
        try:
            start_dt = datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=BRAZIL_TZ)
        except ValueError:
            print(f"[ERROR] Invalid --from date format. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        # End date is yesterday (exclusive of today)
        end_dt_default = today_start - timedelta(days=1)
        start_dt = end_dt_default - timedelta(days=days - 1)
    
    if date_to:
        try:
            end_dt = datetime.strptime(date_to, "%Y-%m-%d").replace(tzinfo=BRAZIL_TZ)
        except ValueError:
            print(f"[ERROR] Invalid --to date format. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        # Default to yesterday
        end_dt = today_start - timedelta(days=1)
    
    return start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")


def read_search_terms(csv_path: str) -> List[str]:
    """Read search terms from CSV file."""
    if not os.path.exists(csv_path):
        print(f"[WARN] Search file not found: {csv_path}")
        return []

    search_terms = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Try different column names
            term = (row.get("Search term") or row.get("search_term") or row.get("keyword") 
                    or row.get("term") or row.get("palavra_chave") or "").strip()
            term = term.strip('"').strip("'").strip(",").strip()
            if term:
                search_terms.append(term.lower())

    return search_terms


def read_tender_ids(csv_path: str) -> List[TenderInfo]:
    """Read tender IDs from CSV file."""
    if not os.path.exists(csv_path):
        print(f"[ERROR] Tender ID file not found: {csv_path}")
        return []

    tenders = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Try different column names for CN number
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


def request_with_retry(
    session: requests.Session,
    url: str,
    params: Dict[str, Any],
    cfg: FetchConfig,
) -> requests.Response:
    last_exc: Optional[Exception] = None
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
    raise RuntimeError(f"Request failed after retries. Last error: {last_exc!r}")


def extract_cn_number(tender: Dict[str, Any]) -> Optional[str]:
    """Extract CN number from tender data."""
    cn = tender.get("numeroControlePNCP") or tender.get("numero_controle_pncp")
    if cn:
        return str(cn).strip()
    
    orgao = tender.get("cnpj") or tender.get("cnpjOrgao") or tender.get("orgaoCnpj") or tender.get("orgao_cnpj")
    ano = tender.get("anoCompra") or tender.get("ano")
    seq = tender.get("sequencialCompra") or tender.get("numero_sequencial")
    
    if orgao and ano and seq:
        return f"{orgao}-1-{seq}/{ano}"
    
    return None


def extract_tender_info(tender: Dict[str, Any]) -> Optional[TenderInfo]:
    """Extract minimal tender info for Input.csv."""
    cn_number = extract_cn_number(tender)
    if not cn_number:
        return None
    
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


def matches_search_terms(tender: Dict[str, Any], search_terms: List[str]) -> bool:
    """Check if tender matches any search term in title/objeto."""
    if not search_terms:
        return True
    
    # Get text fields to search
    texto = " ".join([
        str(tender.get("objetoCompra", "")),
        str(tender.get("objeto", "")),
        str(tender.get("title", "")),
        str(tender.get("description", "")),
        str(tender.get("orgaoEntidade", "")),
        str(tender.get("razaoSocial", "")),
    ]).lower()
    
    return any(term in texto for term in search_terms)


def fetch_all_tenders(
    date_start: str,
    date_end: str,
    cfg: FetchConfig,
    search_terms: Optional[List[str]] = None
) -> List[TenderInfo]:
    """Fetch ALL tenders from the consulta API with optional title filtering."""
    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "Quad99-PNCP-Fetch/1.0 (+https://quad99.com)",
    })

    all_tenders: List[TenderInfo] = []
    seen_cns: Set[str] = set()

    MODALITIES = [1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
    MODALITY_NAMES = {
        1: "Leilão - Loss Special",
        4: "Concorrência",
        5: "Diálogo Competitivo",
        6: "Pregão Eletrônico",
        7: "Pregão Presencial",
        8: "Dispensa",
        9: "Inexigibilidade",
        10: "Pré-qualificação",
        11: "Credenciamento",
        12: "Manifestação de Interesse",
        13: "Leilão",
    }

    mode_str = "SEARCH (with keywords)" if search_terms else "ALL (no filter)"
    print(f"\n{'='*65}")
    print(f"  PNCP Tender Collection ({mode_str})")
    print(f"  Date range: {date_start[:4]}-{date_start[4:6]}-{date_start[6:]} to {date_end[:4]}-{date_end[4:6]}-{date_end[6:]}")
    if search_terms:
        print(f"  Keywords: {', '.join(search_terms[:5])}{'...' if len(search_terms) > 5 else ''}")
    print(f"{'='*65}")

    for mod in MODALITIES:
        pagina = 1
        mod_count = 0
        mod_filtered = 0

        while True:
            params = {
                "dataInicial": date_start,
                "dataFinal": date_end,
                "codigoModalidadeContratacao": mod,
                "pagina": pagina,
                "tamanhoPagina": min(cfg.tamanho_pagina, 50),
            }

            resp = request_with_retry(session, BASE_URL, params, cfg)

            if resp.status_code == 400:
                msg = resp.text[:500]
                print(f"  [WARN] Modality {mod} page {pagina}: 400 error - {msg}")
                break

            if resp.status_code != 200:
                print(f"  [WARN] Modality {mod} page {pagina}: HTTP {resp.status_code}")
                break

            data = resp.json()
            items = data.get("data", []) if isinstance(data, dict) else []

            if not items:
                break

            for it in items:
                # Apply title/keyword filter if search terms provided
                if search_terms and not matches_search_terms(it, search_terms):
                    continue
                
                tender_info = extract_tender_info(it)
                if tender_info and tender_info.cn_number not in seen_cns:
                    seen_cns.add(tender_info.cn_number)
                    all_tenders.append(tender_info)
                    mod_filtered += 1
                mod_count += 1

            total_pages = data.get("totalPaginas", 1)
            if pagina >= total_pages:
                break

            pagina += 1
            time.sleep(cfg.sleep_between_pages_sec)

        mod_name = MODALITY_NAMES.get(mod, f"Code {mod}")
        if search_terms:
            print(f"  Modality {mod:2d} ({mod_name:28s}): {mod_filtered:,}/{mod_count:,} matched")
        else:
            print(f"  Modality {mod:2d} ({mod_name:28s}): {mod_count:,} tenders")

    print(f"{'='*65}")
    print(f"  TOTAL: {len(all_tenders):,} unique tenders")
    print(f"{'='*65}\n")

    return all_tenders


def fetch_search_tenders(
    date_start: str,
    date_end: str,
    search_terms: List[str],
    cfg: FetchConfig
) -> List[TenderInfo]:
    """Fetch tenders matching search terms from the search API."""
    if not search_terms:
        print("[WARN] No search terms provided, falling back to ALL mode")
        return fetch_all_tenders(date_start, date_end, cfg)

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://pncp.gov.br/app/editais",
    })

    all_tenders: List[TenderInfo] = []
    seen_cns: Set[str] = set()

    date_start_dt = datetime.strptime(date_start, "%Y%m%d").replace(tzinfo=BRAZIL_TZ)
    date_end_dt = datetime.strptime(date_end, "%Y%m%d").replace(tzinfo=BRAZIL_TZ)
    date_end_dt = date_end_dt.replace(hour=23, minute=59, second=59)

    PAGE_SIZE = 500

    print(f"\n{'='*80}")
    print(f"  PNCP Tender Collection (SEARCH API Mode)")
    print(f"  Date range: {date_start[:4]}-{date_start[4:6]}-{date_start[6:]} to {date_end[:4]}-{date_end[4:6]}-{date_end[6:]}")
    print(f"  Search terms: {len(search_terms)}")
    print(f"{'='*80}")

    term_stats: List[tuple[str, int, int]] = []

    for search_term in search_terms:
        pagina = 1
        api_count = 0
        filtered_count = 0

        while True:
            params = {
                "q": search_term,
                "tipos_documento": "edital",
                "ordenacao": "-data",
                "status": "todos",
                "pagina": pagina,
                "tam_pagina": PAGE_SIZE,
            }

            resp = request_with_retry(session, SEARCH_URL, params, cfg)

            if resp.status_code != 200:
                break

            data = resp.json()
            items = data.get("items", [])

            if not items:
                break

            stop_pagination = False
            for it in items:
                api_count += 1

                pub_date_str = it.get("data_publicacao_pncp", "")
                if pub_date_str:
                    try:
                        pub_date = datetime.fromisoformat(pub_date_str.replace("Z", "+00:00"))
                        pub_date_br = pub_date.astimezone(BRAZIL_TZ)
                        if pub_date_br < date_start_dt:
                            stop_pagination = True
                            break
                        if not (date_start_dt <= pub_date_br <= date_end_dt):
                            continue
                    except Exception:
                        pass

                filtered_count += 1

                tender_info = extract_tender_info(it)
                if tender_info and tender_info.cn_number not in seen_cns:
                    seen_cns.add(tender_info.cn_number)
                    all_tenders.append(tender_info)

            if stop_pagination:
                break

            total = data.get("total", 0)
            if pagina * PAGE_SIZE >= total:
                break

            pagina += 1
            time.sleep(cfg.sleep_between_pages_sec)

        term_stats.append((search_term.upper(), api_count, filtered_count))

    # Print summary
    print(f"\n  {'Search Term':<25} {'API Results':>12} {'In Date Range':>13} {'Unique':>10}")
    print(f"  {'-'*25} {'-'*12} {'-'*13} {'-'*10}")
    total_api = 0
    total_filtered = 0
    for term, api_cnt, filt_cnt in term_stats:
        if api_cnt > 0 or filt_cnt > 0:
            print(f"  {term:<25} {api_cnt:>12,} {filt_cnt:>13,} {filt_cnt:>10,}")
            total_api += api_cnt
            total_filtered += filt_cnt
    print(f"  {'-'*25} {'-'*12} {'-'*13} {'-'*10}")
    print(f"  {'TOTAL':<25} {total_api:>12,} {total_filtered:>13,} {len(all_tenders):>10,}")
    print(f"{'='*80}\n")

    return all_tenders


def write_input_csv(tenders: List[TenderInfo], out_path: str) -> None:
    """Write CN numbers to Input.csv for GetData.py."""
    if not tenders:
        print("[WARN] No tenders to write")
        return

    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["CN Number"])
        for t in tenders:
            w.writerow([t.cn_number])

    print(f"[OK] Wrote {len(tenders)} CN numbers to: {out_path}")


def write_collected_csv(tenders: List[TenderInfo], out_path: str) -> None:
    """Write collected tenders with details to a summary CSV."""
    if not tenders:
        return

    columns = [
        "CN Number",
        "Numero Controle PNCP",
        "Orgao CNPJ",
        "Ano Compra",
        "Sequencial Compra",
        "Objeto",
        "Orgao Nome",
        "Data Publicacao",
        "Modalidade",
    ]

    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(columns)
        for t in tenders:
            w.writerow([
                t.cn_number,
                t.numero_controle_pncp,
                t.orgao_cnpj,
                t.ano_compra,
                t.sequencial_compra,
                t.objeto,
                t.orgao_nome,
                t.data_publicacao,
                t.modalidade,
            ])

    print(f"[OK] Wrote collected tenders summary to: {out_path}")


def run_getdata(script_dir: Path) -> int:
    """Run GetData.py to process the Input.csv."""
    getdata_path = script_dir / "GetData.py"
    
    if not getdata_path.exists():
        print(f"[ERROR] GetData.py not found at: {getdata_path}")
        return 1

    print(f"\n{'='*65}")
    print(f"  Running GetData.py to fetch full tender details...")
    print(f"{'='*65}\n")

    try:
        result = subprocess.run(
            [sys.executable, "-u", str(getdata_path)],
            cwd=str(script_dir),
            check=True
        )
        print(f"\n[OK] GetData.py completed successfully")
        return result.returncode
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] GetData.py failed with exit code: {e.returncode}")
        return e.returncode
    except Exception as e:
        print(f"\n[ERROR] Failed to run GetData.py: {e}")
        return 1


def main() -> int:
    days, mode, search_file, input_csv, id_file, auto_run, date_from, date_to = parse_args()
    script_dir = Path(__file__).parent

    print(f"[INFO] Configuration:")
    print(f"       Mode: {mode}")
    print(f"       Days: {days}")
    if date_from:
        print(f"       Date from: {date_from}")
    if date_to:
        print(f"       Date to: {date_to}")
    print(f"       Search file: {search_file}")
    print(f"       ID file: {id_file}")
    print(f"       Input CSV: {input_csv}")
    print(f"       Auto-run GetData.py: {auto_run}")

    if mode not in ("all", "search", "ids"):
        print(f"[ERROR] Invalid mode '{mode}'. Use 'all', 'search', or 'ids'")
        return 1

    # Handle IDS mode - read directly from file
    if mode == "ids":
        id_path = script_dir / id_file
        tenders = read_tender_ids(str(id_path))
        print(f"[INFO] Read {len(tenders)} tender IDs from {id_path}")
    else:
        # Calculate date range
        date_start, date_end = get_date_range(days, date_from, date_to)
        cfg = FetchConfig()

        if mode == "search":
            search_terms = read_search_terms(search_file)
            if not search_terms:
                print(f"[WARN] No search terms found in {search_file}, switching to ALL mode")
                tenders = fetch_all_tenders(date_start, date_end, cfg)
            else:
                tenders = fetch_search_tenders(date_start, date_end, search_terms, cfg)
        else:  # mode == "all"
            tenders = fetch_all_tenders(date_start, date_end, cfg)

    if not tenders:
        print("[WARN] No tenders found")
        return 0

    # Write Input.csv for GetData.py
    input_path = script_dir / input_csv
    write_input_csv(tenders, str(input_path))

    # Also write a summary CSV for reference
    collected_path = script_dir / "collected_tenders_summary.csv"
    write_collected_csv(tenders, str(collected_path))

    # Auto-run GetData.py if requested
    if auto_run:
        return run_getdata(script_dir)
    else:
        print(f"\n[INFO] To fetch full tender details, run:")
        print(f"       python GetData.py")
        print(f"\n       Or run this script with -r flag:")
        if mode == "ids":
            print(f"       python PreviouDayTender.py -m ids -r")
        else:
            print(f"       python PreviouDayTender.py -d {days} -r")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
