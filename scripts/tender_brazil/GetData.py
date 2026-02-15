# pncp_scraper.py
# Usage:
#   python pncp_scraper.py "https://pncp.gov.br/app/editais/11308823000103/2025/33" output.csv
#
# What it does:
# - Fetch tender/header info (title/object, authority, dates, modality, legal basis, etc.)
# - Fetch all items (lot list) via PNCP API
# - For each item, fetch award results (if present) via PNCP API
# - Output ONE ROW PER (item x award_result). If no award results, outputs one row with award fields blank.

from __future__ import annotations

import csv
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

# -- Config loader (reads from config/Tender_Brazil.env.json) --
_script_dir = Path(__file__).resolve().parent
_repo_root = _script_dir.parents[1]
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from config_loader import load_env_file, getenv, getenv_int  # noqa: E402

load_env_file()

# -----------------------------
# Config
# -----------------------------
DEFAULT_TIMEOUT = getenv_int("DEFAULT_TIMEOUT", 60)

# Tor proxy setup (optional — works without Tor too)
TOR_PROXIES = None
try:
    from core.network.tor_httpx import TorConfig, setup_tor
    _tor_cfg = TorConfig.from_env(getenv_fn=getenv)
    _tor_proxy_url = setup_tor(_tor_cfg)
    if _tor_proxy_url:
        # requests uses socks5h:// (h = DNS through proxy too)
        TOR_PROXIES = {
            "http": _tor_proxy_url.replace("socks5://", "socks5h://"),
            "https": _tor_proxy_url.replace("socks5://", "socks5h://"),
        }
except Exception as e:
    print(f"[WARN] Tor setup failed, proceeding without proxy: {e}")

# DAYS = 0  -> read CN numbers from Input.csv
# DAYS = 1  -> fetch tenders from yesterday only (excludes today)
# DAYS = N  -> fetch tenders from the last N days (excludes today)
# DAYS = -1 -> fetch ALL tenders (no date filter, search API only)
DAYS = getenv_int("DAYS", 0)

OUTPUT_COLUMNS = [
    "COUNTRY",
    "PROVINCE",
    "SOURCE",
    "Source Tender Id",
    "Tender Title",
    "Lot Number",
    "Sub Lot Number",
    "Lot Title",
    "Awarded Lot Title",
    "Est Lot Value Local",
    "Local Currency",
    "Deadline Date",
    "TENDERING AUTHORITY",
    "Status",
    "CN Document Number",
    "Original_Publication_Link_Notice",
    "Ceiling Price Per MG/IU",
    "Ceiling Price Per Pack",
    "Ceiling Unit Price",
    "MEAT",
    "Price Evaluation ratio",
    "Quality Evaluation ratio",
    "Other Evaluation ratio",
    "CAN Document Number",
    "Award Date",
    "Bidder",
    "Bid Status Award",
    "Lot_Award_Value_Local",
    "Awarded Unit Price",
    "Price Evaluation",
    "Quality Evaluation",
    "Other Evaluation",
    "Original_Publication_Link_Award",
    "Type Of Contract",
    "Legal basis",
    "Budget source",
    "Purchasing Unit",
    "Publication date",
    "Ranking Order",
    "Supplier Identification Number",
    "Item No.",
    "Basic productive incentive",
    "Awarded Quantity",
]


# -----------------------------
# Helpers
# -----------------------------
def _safe_get(d: Dict[str, Any], *keys: str) -> Optional[Any]:
    """Try multiple possible keys (API sometimes changes names)."""
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _as_str(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _parse_iso_date(v: Any) -> str:
    """
    PNCP API often returns ISO like '2026-01-28T11:42:56' or '2025-12-23T19:07:33'.
    Output: DD-MM-YYYY (matches your sample)
    """
    if not v:
        return ""
    s = str(v).strip()
    # If it already looks like DD-MM-YYYY, keep
    if re.match(r"^\d{2}-\d{2}-\d{4}$", s):
        return s
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.strftime("%d-%m-%Y")
    except Exception:
        # Try just YYYY-MM-DD
        try:
            dt = datetime.strptime(s[:10], "%Y-%m-%d")
            return dt.strftime("%d-%m-%Y")
        except Exception:
            return s


def _detect_province(local_text: str) -> str:
    """
    Example: 'Afogados da Ingazeira/PE' -> 'PE'
    Example: 'Resende/RJ' -> 'RJ'
    """
    if not local_text:
        return ""
    m = re.search(r"/([A-Z]{2})\b", local_text.strip())
    return m.group(1) if m else ""


def _normalize_title(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def parse_editais_url(url: str) -> Tuple[str, str, str]:
    """
    URL format:
      https://pncp.gov.br/app/editais/{orgao}/{ano}/{compra}
    Return: (orgao, ano, compra)
    """
    p = urlparse(url)
    parts = [x for x in p.path.split("/") if x]
    # expect ["app","editais",orgao,ano,compra]
    if len(parts) >= 5 and parts[0] == "app" and parts[1] == "editais":
        return parts[2], parts[3], parts[4]
    raise ValueError(f"Unexpected PNCP editais URL format: {url}")


def parse_cn_number(cn_number: str) -> Tuple[str, str, str]:
    """
    CN Number format:
      {orgao}-1-{compra}/{ano}
    Example: 11800731000138-1-000129/2025
    Return: (orgao, ano, compra)
    """
    cn_number = cn_number.strip()
    # Pattern: {orgao}-1-{compra}/{ano}
    match = re.match(r"^(\d+)-1-(\d+)/(\d{4})$", cn_number)
    if match:
        orgao, compra, ano = match.groups()
        return orgao, ano, compra
    raise ValueError(f"Unexpected CN Number format: {cn_number}")


def read_cn_numbers_from_csv(csv_path: str) -> List[str]:
    """
    Read CN numbers from Input.csv file.
    Expected format: CSV with header "CN Number" and CN numbers in rows.
    Returns empty list if file doesn't exist.
    """
    cn_numbers = []
    if not os.path.exists(csv_path):
        print(f"[WARN] Input CSV file not found: {csv_path}")
        return cn_numbers

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cn = row.get("CN Number", "").strip()
            if cn:
                cn_numbers.append(cn)

    return cn_numbers


def read_search_terms(csv_path: str) -> List[str]:
    """
    Read search terms from SearchTerm.csv.
    Returns list of lowercase search terms for case-insensitive matching.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"SearchTerm CSV file not found: {csv_path}")

    search_terms = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:  # utf-8-sig handles BOM
        reader = csv.DictReader(f)
        for row in reader:
            term = row.get("Search term", "").strip()
            # Clean quotes and whitespace
            term = term.strip('"').strip("'").strip(",").strip()
            if term:
                search_terms.append(term.lower())

    return search_terms


def contains_search_term(text_fields: List[str], search_terms: List[str]) -> bool:
    """
    Check if any search term appears in any of the text fields (case-insensitive).
    Returns True if at least one search term is found.
    """
    combined_text = " ".join(str(f) for f in text_fields if f).lower()
    return any(term in combined_text for term in search_terms)


def fetch_cn_numbers_from_search_api(days: int) -> List[str]:
    """
    Fetch CN numbers from the PNCP search API using keywords from SearchTerm.csv,
    filtered by publication date (last N days, excluding today).

    Uses /api/search/ endpoint - searches each keyword separately, trusts API results,
    only filters by date range (client-side since API doesn't support date params).

    If days=-1, no date filtering is applied (fetches all results for search terms).
    """
    from zoneinfo import ZoneInfo

    BRAZIL_TZ = ZoneInfo("America/Sao_Paulo")
    now_br = datetime.now(tz=BRAZIL_TZ)
    today_start = now_br.replace(hour=0, minute=0, second=0, microsecond=0)

    # Date range: excludes today, goes back N days (-1 = no filter)
    apply_date_filter = days > 0
    if apply_date_filter:
        # Yesterday = today - 1 day, but we need the full day (00:00 to 23:59:59)
        date_end_dt = today_start  # End of range is start of today (exclusive)
        date_start_dt = today_start - timedelta(days=days)  # N days ago
    else:
        date_end_dt = None
        date_start_dt = None

    SEARCH_URL = "https://pncp.gov.br/api/search/"
    PAGE_SIZE = 500

    # Read search terms from CSV
    script_dir = Path(__file__).parent
    search_csv = script_dir / "SearchTerm.csv"
    search_terms = read_search_terms(str(search_csv))

    if not search_terms:
        print("[WARN] No search terms found in SearchTerm.csv")
        return []

    session = requests.Session()
    if TOR_PROXIES:
        session.proxies.update(TOR_PROXIES)
    adapter = requests.adapters.HTTPAdapter(max_retries=3)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://pncp.gov.br/app/editais",
    })

    req_timeout = 120 if TOR_PROXIES else 60

    cn_numbers: List[str] = []
    cn_details: List[Dict[str, Any]] = []  # store full search records
    seen: set = set()

    print(f"\n{'='*80}")
    print(f"  PNCP Search API Collection")
    if apply_date_filter:
        # Show inclusive range (end_dt is exclusive, so show end_dt - 1 day)
        display_end = date_end_dt - timedelta(days=1)
        print(f"  Date range : {date_start_dt.strftime('%Y-%m-%d')} to {display_end.strftime('%Y-%m-%d')} (last {days} day{'s' if days > 1 else ''})")
    else:
        print(f"  Date range : ALL (no date filter)")
    print(f"  Search terms : {len(search_terms)}")
    print(f"{'='*80}")

    term_stats: List[Tuple[str, int, int]] = []  # (term, api_count, filtered_count)

    for search_term in search_terms:
        pagina = 1
        api_count = 0
        filtered_count = 0
        term_cns: List[str] = []

        while True:
            MAX_RETRIES = 5
            resp = None
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    resp = session.get(SEARCH_URL, params={
                        "q": search_term,
                        "tipos_documento": "edital",
                        "ordenacao": "-data",  # newest first
                        "status": "todos",
                        "pagina": pagina,
                        "tam_pagina": PAGE_SIZE,
                    }, timeout=req_timeout)
                    break
                except Exception as e:
                    if attempt < MAX_RETRIES:
                        wait = 5 * attempt
                        print(f"  [RETRY] Term '{search_term}' page {pagina} attempt {attempt}/{MAX_RETRIES} (waiting {wait}s)")
                        time.sleep(wait)
                    else:
                        print(f"  [WARN] Term '{search_term}' page {pagina}: failed after {MAX_RETRIES} retries")

            if resp is None or resp.status_code != 200:
                break

            data = resp.json()
            items = data.get("items", [])

            if not items:
                break

            # Process items with date filtering (if enabled)
            stop_pagination = False
            for it in items:
                api_count += 1

                # Date filter (client-side since API doesn't support date params)
                if apply_date_filter:
                    pub_date_str = it.get("data_publicacao_pncp", "")
                    if pub_date_str:
                        try:
                            pub_date = datetime.fromisoformat(pub_date_str.replace("Z", "+00:00"))
                            pub_date_br = pub_date.astimezone(BRAZIL_TZ)
                            # Since results are sorted newest-first, stop when we hit old dates
                            if pub_date_br < date_start_dt:
                                stop_pagination = True
                                break
                            # Skip if not in date range (date_start <= date < date_end)
                            if not (date_start_dt <= pub_date_br < date_end_dt):
                                continue
                        except Exception:
                            pass  # if date parsing fails, include item anyway

                # Trust search API results - it already filtered by the search term
                # No secondary keyword filter needed
                filtered_count += 1

                # Collect CN number
                cn = it.get("numero_controle_pncp", "")
                if cn and cn not in seen:
                    seen.add(cn)
                    cn_numbers.append(cn)
                    cn_details.append(it)
                    term_cns.append(cn)

            if stop_pagination:
                break

            # Check if there are more pages (estimate based on total and page size)
            total = data.get("total", 0)
            if pagina * PAGE_SIZE >= total:
                break

            pagina += 1
            time.sleep(0.3)

        term_stats.append((search_term.upper(), api_count, filtered_count))

    # Print summary table
    print(f"\n  {'Search Term':<20} {'API Results':>12} {'In Date Range':>13} {'Unique CNs':>11}")
    print(f"  {'-'*20} {'-'*12} {'-'*13} {'-'*11}")
    total_api = 0
    total_filtered = 0
    for term, api_cnt, filt_cnt in term_stats:
        if api_cnt > 0 or filt_cnt > 0:
            print(f"  {term:<20} {api_cnt:>12,} {filt_cnt:>13,} {filt_cnt:>11,}")
            total_api += api_cnt
            total_filtered += filt_cnt
    print(f"  {'-'*20} {'-'*12} {'-'*13} {'-'*11}")
    print(f"  {'TOTAL':<20} {total_api:>12,} {total_filtered:>13,} {len(cn_numbers):>11,}")
    print(f"{'='*80}\n")

    # Write collected tenders to CSV with search API details
    if cn_details:
        consulta_csv = Path(__file__).parent / "collected_tenders.csv"
        _write_search_csv(cn_details, str(consulta_csv))
        print(f"[INFO] Tender list saved to {consulta_csv}")

    return cn_numbers


def fetch_cn_numbers_from_api(days: int) -> List[str]:
    """
    Fetch CN numbers from the PNCP consulta API for tenders published
    in the last `days` days (excluding today).

    Uses /api/consulta/v1/contratacoes/publicacao with server-side date
    filtering via dataInicial/dataFinal + codigoModalidadeContratacao.
    """
    from zoneinfo import ZoneInfo

    BRAZIL_TZ = ZoneInfo("America/Sao_Paulo")
    now_br = datetime.now(tz=BRAZIL_TZ)
    today_start = now_br.replace(hour=0, minute=0, second=0, microsecond=0)

    date_end = (today_start - timedelta(days=1)).strftime("%Y%m%d")       # yesterday
    date_start = (today_start - timedelta(days=days)).strftime("%Y%m%d")  # N days ago

    CONSULTA_URL = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    PAGE_SIZE = 50  # API max
    # Active modality codes (tested: 2,3,14,15 return empty/error)
    MODALITIES = [1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]

    session = requests.Session()
    if TOR_PROXIES:
        session.proxies.update(TOR_PROXIES)
    # Auto-retry on connection errors at transport level
    adapter = requests.adapters.HTTPAdapter(max_retries=3)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://pncp.gov.br/app/editais",
    })

    req_timeout = 120 if TOR_PROXIES else 60

    cn_numbers: List[str] = []
    cn_details: List[Dict[str, Any]] = []  # store full consulta records
    seen: set = set()

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

    print(f"\n{'='*65}")
    print(f"  PNCP Tender Collection")
    print(f"  Date range : {date_start[:4]}-{date_start[4:6]}-{date_start[6:]} to {date_end[:4]}-{date_end[4:6]}-{date_end[6:]}")
    print(f"  Modalities : {len(MODALITIES)}")
    print(f"{'='*65}")

    modality_stats: List[Tuple[int, str, int, int]] = []  # (code, name, cn_count, total_registered)

    for mod in MODALITIES:
        pagina = 1
        mod_count = 0
        total_registered = 0

        while True:
            MAX_RETRIES = 5
            resp = None
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    resp = session.get(CONSULTA_URL, params={
                        "dataInicial": date_start,
                        "dataFinal": date_end,
                        "codigoModalidadeContratacao": mod,
                        "pagina": pagina,
                        "tamanhoPagina": PAGE_SIZE,
                    }, timeout=req_timeout)
                    break  # success
                except Exception as e:
                    if attempt < MAX_RETRIES:
                        wait = 5 * attempt
                        print(f"  [RETRY] Modality {mod} page {pagina} attempt {attempt}/{MAX_RETRIES} (waiting {wait}s)")
                        time.sleep(wait)
                    else:
                        print(f"  [WARN] Modality {mod} page {pagina}: failed after {MAX_RETRIES} retries")

            if resp is None or resp.status_code != 200:
                break

            data = resp.json()
            items = data.get("data", [])

            if pagina == 1:
                total_registered = data.get("totalRegistros", 0)

            if not items:
                break

            for it in items:
                cn = it.get("numeroControlePNCP", "")
                if cn and cn not in seen:
                    seen.add(cn)
                    cn_numbers.append(cn)
                    cn_details.append(it)
                    mod_count += 1

            total_pages = data.get("totalPaginas", 1)
            if pagina >= total_pages:
                break

            pagina += 1
            time.sleep(0.3)

        mod_name = MODALITY_NAMES.get(mod, f"Code {mod}")
        modality_stats.append((mod, mod_name, mod_count, total_registered))

    # Print summary table
    print(f"\n  {'Code':<6} {'Modality':<28} {'Records':>8} {'Unique CNs':>11}")
    print(f"  {'-'*6} {'-'*28} {'-'*8} {'-'*11}")
    total_records = 0
    for code, name, cn_count, total_reg in modality_stats:
        if total_reg > 0 or cn_count > 0:
            print(f"  {code:<6} {name:<28} {total_reg:>8,} {cn_count:>11,}")
            total_records += total_reg
    print(f"  {'-'*6} {'-'*28} {'-'*8} {'-'*11}")
    print(f"  {'':6} {'TOTAL':<28} {total_records:>8,} {len(cn_numbers):>11,}")
    print(f"{'='*65}\n")

    # Write collected tenders to CSV with all consulta API details
    if cn_details:
        consulta_csv = Path(__file__).parent / "collected_tenders.csv"
        _write_consulta_csv(cn_details, str(consulta_csv))
        print(f"[INFO] Tender list saved to {consulta_csv}")

    return cn_numbers


CONSULTA_CSV_COLUMNS = [
    "numeroControlePNCP",
    "processo",
    "objetoCompra",
    "orgao_cnpj",
    "orgao_razaoSocial",
    "orgao_esferaId",
    "orgao_poderId",
    "unidade_nomeUnidade",
    "unidade_ufSigla",
    "unidade_municipioNome",
    "modalidadeId",
    "modalidadeNome",
    "situacaoCompraNome",
    "valorTotalEstimado",
    "valorTotalHomologado",
    "srp",
    "modoDisputaNome",
    "amparoLegal_nome",
    "dataPublicacaoPncp",
    "dataAberturaProposta",
    "dataEncerramentoProposta",
    "dataAtualizacao",
    "linkSistemaOrigem",
    "anoCompra",
    "sequencialCompra",
    "numeroCompra",
]


def _write_consulta_csv(records: List[Dict[str, Any]], path: str) -> None:
    """Flatten nested consulta API records and write to CSV."""
    def _flatten(rec: Dict[str, Any]) -> Dict[str, str]:
        org = rec.get("orgaoEntidade") or {}
        unit = rec.get("unidadeOrgao") or {}
        amparo = rec.get("amparoLegal") or {}
        return {
            "numeroControlePNCP": _as_str(rec.get("numeroControlePNCP")),
            "processo": _as_str(rec.get("processo")),
            "objetoCompra": _as_str(rec.get("objetoCompra")),
            "orgao_cnpj": _as_str(org.get("cnpj")),
            "orgao_razaoSocial": _as_str(org.get("razaoSocial")),
            "orgao_esferaId": _as_str(org.get("esferaId")),
            "orgao_poderId": _as_str(org.get("poderId")),
            "unidade_nomeUnidade": _as_str(unit.get("nomeUnidade")),
            "unidade_ufSigla": _as_str(unit.get("ufSigla")),
            "unidade_municipioNome": _as_str(unit.get("municipioNome")),
            "modalidadeId": _as_str(rec.get("modalidadeId")),
            "modalidadeNome": _as_str(rec.get("modalidadeNome")),
            "situacaoCompraNome": _as_str(rec.get("situacaoCompraNome")),
            "valorTotalEstimado": _as_str(rec.get("valorTotalEstimado")),
            "valorTotalHomologado": _as_str(rec.get("valorTotalHomologado")),
            "srp": _as_str(rec.get("srp")),
            "modoDisputaNome": _as_str(rec.get("modoDisputaNome")),
            "amparoLegal_nome": _as_str(amparo.get("nome")),
            "dataPublicacaoPncp": _as_str(rec.get("dataPublicacaoPncp")),
            "dataAberturaProposta": _as_str(rec.get("dataAberturaProposta")),
            "dataEncerramentoProposta": _as_str(rec.get("dataEncerramentoProposta")),
            "dataAtualizacao": _as_str(rec.get("dataAtualizacao")),
            "linkSistemaOrigem": _as_str(rec.get("linkSistemaOrigem")),
            "anoCompra": _as_str(rec.get("anoCompra")),
            "sequencialCompra": _as_str(rec.get("sequencialCompra")),
            "numeroCompra": _as_str(rec.get("numeroCompra")),
        }

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=CONSULTA_CSV_COLUMNS)
        w.writeheader()
        for rec in records:
            w.writerow(_flatten(rec))


def _write_search_csv(records: List[Dict[str, Any]], path: str) -> None:
    """Map search API records to consulta CSV format for consistency."""
    def _map_search_to_consulta(rec: Dict[str, Any]) -> Dict[str, str]:
        # Search API has flatter structure than consulta API
        return {
            "numeroControlePNCP": _as_str(rec.get("numero_controle_pncp")),
            "processo": _as_str(rec.get("numero")),
            "objetoCompra": _as_str(rec.get("title") or rec.get("description")),
            "orgao_cnpj": _as_str(rec.get("orgao_cnpj")),
            "orgao_razaoSocial": _as_str(rec.get("orgao_nome")),
            "orgao_esferaId": _as_str(rec.get("esfera_id")),
            "orgao_poderId": _as_str(rec.get("poder_id")),
            "unidade_nomeUnidade": _as_str(rec.get("unidade_nome")),
            "unidade_ufSigla": _as_str(rec.get("uf")),
            "unidade_municipioNome": _as_str(rec.get("municipio_nome")),
            "modalidadeId": _as_str(rec.get("modalidade_licitacao_id")),
            "modalidadeNome": _as_str(rec.get("modalidade_licitacao_nome")),
            "situacaoCompraNome": _as_str(rec.get("situacao_nome")),
            "valorTotalEstimado": _as_str(rec.get("valor_global")),
            "valorTotalHomologado": "",
            "srp": "",
            "modoDisputaNome": "",
            "amparoLegal_nome": "",
            "dataPublicacaoPncp": _as_str(rec.get("data_publicacao_pncp")),
            "dataAberturaProposta": "",
            "dataEncerramentoProposta": "",
            "dataAtualizacao": _as_str(rec.get("data_atualizacao_pncp")),
            "linkSistemaOrigem": _as_str(rec.get("item_url")),
            "anoCompra": _as_str(rec.get("ano")),
            "sequencialCompra": _as_str(rec.get("numero_sequencial")),
            "numeroCompra": _as_str(rec.get("numero")),
        }

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=CONSULTA_CSV_COLUMNS)
        w.writeheader()
        for rec in records:
            w.writerow(_map_search_to_consulta(rec))


def build_editais_url(orgao: str, ano: str, compra: str) -> str:
    """
    Build PNCP editais URL from components.
    Format: https://pncp.gov.br/app/editais/{orgao}/{ano}/{compra}
    """
    return f"https://pncp.gov.br/app/editais/{orgao}/{ano}/{compra}"


@dataclass
class TenderHeader:
    country: str = "BRAZIL"
    source: str = "PNCP"
    province: str = ""
    source_tender_id: str = ""
    tender_title: str = ""
    status: str = ""
    cn_document_number: str = ""
    publication_link_notice: str = ""
    tendering_authority: str = ""
    purchasing_unit: str = ""
    type_of_contract: str = ""
    legal_basis: str = ""
    budget_source: str = ""
    publication_date: str = ""
    deadline_date: str = ""
    local_currency: str = "BRL"
    est_total_purchase_value: str = ""      # not required in your final columns, but kept if needed
    total_approved_purchase_value: str = "" # not required in your final columns, but kept if needed


class PNCPClient:
    def __init__(self, session: Optional[requests.Session] = None):
        self.s = session or requests.Session()
        if TOR_PROXIES:
            self.s.proxies.update(TOR_PROXIES)
        self.s.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://pncp.gov.br/app/editais",
        })

    def get_json(self, url: str) -> Any:
        r = self.s.get(url, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status()
        return r.json()

    def get_text(self, url: str) -> str:
        r = self.s.get(url, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status()
        return r.text


def build_api_base(orgao: str) -> str:
    # As shown in your file: https://pncp.gov.br/api/pncp/v1/orgaos/{orgao}/... :contentReference[oaicite:2]{index=2}
    return f"https://pncp.gov.br/api/pncp/v1/orgaos/{orgao}"


def api_items_url(orgao: str, ano: str, compra: str, page: int = 1, page_size: int = 1000) -> str:
    # As shown in your file: .../compras/2025/33/itens?pagina=1&tamanhoPagina=1000 :contentReference[oaicite:3]{index=3}
    return f"{build_api_base(orgao)}/compras/{ano}/{compra}/itens?pagina={page}&tamanhoPagina={page_size}"


def api_item_results_url(orgao: str, ano: str, compra: str, item_number: int) -> str:
    # As shown in your file: .../itens/6906477/resultados :contentReference[oaicite:4]{index=4}
    return f"{build_api_base(orgao)}/compras/{ano}/{compra}/itens/{item_number}/resultados"


def api_purchase_url(orgao: str, ano: str, compra: str) -> str:
    # Not shown in the snippet, but PNCP commonly exposes purchase details at:
    # .../compras/{ano}/{compra}
    return f"{build_api_base(orgao)}/compras/{ano}/{compra}"


def extract_header_from_api(purchase_json: Dict[str, Any], notice_url: str, cn_number: str = "") -> TenderHeader:
    """
    Tries to map PNCP purchase JSON to your header fields.
    PNCP schemas vary, so we try multiple key variants.
    """
    h = TenderHeader()
    h.publication_link_notice = notice_url

    # CN Document Number: use provided CN number or extract from API (numeroControlePNCPCompra)
    if cn_number:
        h.cn_document_number = cn_number
    else:
        h.cn_document_number = _as_str(_safe_get(purchase_json, "numeroControlePNCPCompra", "idContratacaoPNCP", "idContratacaoPncp", "idContratacao"))
    
    # Source Tender Id: try to get process number (e.g., "PE 235/2025")
    # This might be in "numeroProcesso" or similar field
    h.source_tender_id = _as_str(_safe_get(purchase_json, "numeroProcesso", "numeroCompra", "numero", "idCompra", "processo"))
    if not h.source_tender_id:
        # Fallback: use compra number if available
        compra_num = _safe_get(purchase_json, "numeroCompra", "numero")
        if compra_num:
            h.source_tender_id = str(compra_num)

    # Local / UF
    local = _as_str(_safe_get(purchase_json, "local", "municipio", "localidade"))
    h.province = _detect_province(local)

    # Authority / Purchasing unit
    h.tendering_authority = _as_str(_safe_get(purchase_json, "orgaoEntidade", "orgao", "nomeOrgao", "nomeEntidade"))
    h.purchasing_unit = _as_str(_safe_get(purchase_json, "unidadeCompradora", "nomeUnidadeCompradora", "unidade"))

    # Title: prefer "objeto" or "descricao" if present
    title = _as_str(_safe_get(purchase_json, "objeto", "descricao", "titulo", "nome"))
    h.tender_title = _normalize_title(title)

    # Type of contract / modality
    h.type_of_contract = _as_str(_safe_get(purchase_json, "modalidadeNome", "modalidade", "modalidadeDaContratacao"))
    # Legal basis
    h.legal_basis = _as_str(_safe_get(purchase_json, "amparoLegal", "amparoLegalNome", "baseLegal"))
    # Budget source
    h.budget_source = _as_str(_safe_get(purchase_json, "fonteOrcamentaria", "fonteOrcamentariaNome", "fonteOrcamento"))
    # Status
    h.status = _as_str(_safe_get(purchase_json, "situacao", "situacaoNome", "status", "statusNome"))

    # Dates
    h.publication_date = _parse_iso_date(_safe_get(purchase_json, "dataPublicacaoPncp", "dataDivulgacaoPncp", "dataPublicacao"))
    h.deadline_date = _parse_iso_date(_safe_get(purchase_json, "dataFimRecebimentoPropostas", "dataFim", "dataEncerramento"))

    return h


def fetch_header(client: PNCPClient, orgao: str, ano: str, compra: str, notice_url: str, cn_number: str = "") -> TenderHeader:
    """
    Best-effort: get header from purchase API.
    If purchase endpoint fails, we still output rows using what we have from URL + defaults.
    """
    try:
        pj = client.get_json(api_purchase_url(orgao, ano, compra))
        return extract_header_from_api(pj, notice_url, cn_number)
    except Exception:
        # minimal fallback
        h = TenderHeader()
        h.publication_link_notice = notice_url
        h.cn_document_number = cn_number if cn_number else ""
        h.source_tender_id = f"{compra}/{ano}"
        return h


def fetch_items(client: PNCPClient, orgao: str, ano: str, compra: str) -> List[Dict[str, Any]]:
    items = client.get_json(api_items_url(orgao, ano, compra, page=1, page_size=1000))
    # API returns list (as shown in your file) :contentReference[oaicite:5]{index=5}
    if isinstance(items, list):
        return items
    # Sometimes paginated response wraps content
    return items.get("content", []) if isinstance(items, dict) else []


def fetch_results_for_item(client: PNCPClient, orgao: str, ano: str, compra: str, item_number: int) -> List[Dict[str, Any]]:
    try:
        res = client.get_json(api_item_results_url(orgao, ano, compra, item_number))
        # API returns list (as shown in your file) :contentReference[oaicite:6]{index=6}
        return res if isinstance(res, list) else res.get("content", [])
    except requests.HTTPError as e:
        # If no results endpoint for that item, treat as no award
        if getattr(e.response, "status_code", None) in (404, 400):
            return []
        raise
    except Exception:
        return []


def build_rows(header: TenderHeader, items: List[Dict[str, Any]], client: PNCPClient,
               orgao: str, ano: str, compra: str, purchase_json: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    out_rows: List[Dict[str, Any]] = []

    # Extract evaluation ratios from purchase data
    price_eval_ratio = ""
    quality_eval_ratio = ""
    other_eval_ratio = ""
    meat_value = "NO"
    
    if purchase_json:
        # Try to extract evaluation criteria/ratios
        criterios = purchase_json.get("criteriosAvaliacao", [])
        if isinstance(criterios, list):
            for crit in criterios:
                nome = _as_str(_safe_get(crit, "nome", "tipo", "criterio")).upper()
                peso = _safe_get(crit, "peso", "percentual", "valor")
                peso_str = _as_str(peso)
                # Remove % if present and extract number
                peso_num = re.sub(r"[^\d]", "", peso_str)
                
                if any(k in nome for k in ["PREÇO", "PRICE", "VALOR", "CUSTO", "COST"]):
                    price_eval_ratio = peso_num if peso_num else ""
                elif any(k in nome for k in ["QUALIDADE", "QUALITY", "TÉCNICO", "TECHNICAL"]):
                    quality_eval_ratio = peso_num if peso_num else ""
                else:
                    if other_eval_ratio:
                        other_eval_ratio = str(int(other_eval_ratio or 0) + int(peso_num or 0))
                    else:
                        other_eval_ratio = peso_num if peso_num else ""
        
        # MEAT (Micro e Pequena Empresa - Small Business preference)
        # Check both purchase level and item level
        meat = _safe_get(purchase_json, "preferenciaME", "preferenciaMicroEmpresa", "meat", "preferenciaMEAT",
                        "aplicacaoBeneficioMeEpp", "beneficioMeEpp")
        if meat:
            meat_value = "YES" if str(meat).lower() in ("true", "1", "yes", "sim") else "NO"
        else:
            # Check if any item has MEAT benefit
            items_data = purchase_json.get("itens", [])
            if isinstance(items_data, list):
                for item in items_data:
                    item_meat = _safe_get(item, "aplicacaoBeneficioMeEpp", "beneficioMeEpp", "preferenciaME")
                    if item_meat and str(item_meat).lower() in ("true", "1", "yes", "sim"):
                        meat_value = "YES"
                        break

    # Business rule: If MEAT is NO, evaluation should be 100% price-based
    if meat_value == "NO" and not price_eval_ratio:
        price_eval_ratio = "100"
        quality_eval_ratio = ""
        other_eval_ratio = ""

    for it in items:
        item_no = _safe_get(it, "itemNumber", "numeroItem", "numero")
        desc = _as_str(_safe_get(it, "Description", "descricao", "description"))
        qty = _safe_get(it, "Quantity", "quantidade", "qtde")
        est_unit = _safe_get(it, "EstimatedUnitValue", "valorUnitarioEstimado", "estimatedUnitValue")
        est_total = _safe_get(it, "totalValue", "valorTotalEstimado", "valorTotal")

        basic_prod = _safe_get(it, "incentivoProdutivoBasico", "basicProductiveIncentive", "incentivoProdutivo")
        
        # Ceiling Price Per MG/IU - try to extract from item data
        # This might need to be calculated or extracted from a specific field
        ceiling_price_mg_iu = _as_str(_safe_get(it, "valorUnitarioPorMG", "valorPorMG", "precoPorMG", "valorUnitarioMG", "precoPorUnidadeMG"))

        # Your sheet treats "Lot Number" as the item number
        lot_number = _as_str(item_no)

        # Base row (used for each award result)
        base = {c: "" for c in OUTPUT_COLUMNS}
        base.update({
            "COUNTRY": header.country,
            "PROVINCE": header.province,
            "SOURCE": header.source,
            "Source Tender Id": header.source_tender_id,
            "Tender Title": header.tender_title,
            "Lot Number": lot_number,
            "Sub Lot Number": "",  # not available in PNCP item API
            "Lot Title": desc,
            "Awarded Lot Title": "",  # will be filled from award if available
            "Est Lot Value Local": _as_str(est_total),
            "Local Currency": header.local_currency,
            "Deadline Date": header.deadline_date,
            "TENDERING AUTHORITY": header.tendering_authority,
            "Status": header.status,
            "CN Document Number": header.cn_document_number,
            "Original_Publication_Link_Notice": header.publication_link_notice,
            "Ceiling Price Per MG/IU": ceiling_price_mg_iu,
            "Ceiling Price Per Pack": "",  # not typically available
            "Ceiling Unit Price": _as_str(est_unit),  # "Valor unitário estimado"
            "MEAT": meat_value,
            "Price Evaluation ratio": price_eval_ratio,
            "Quality Evaluation ratio": quality_eval_ratio,
            "Other Evaluation ratio": other_eval_ratio,
            "CAN Document Number": header.cn_document_number,  # Same as CN Document Number
            "Type Of Contract": header.type_of_contract,
            "Legal basis": header.legal_basis,
            "Budget source": header.budget_source,
            "Purchasing Unit": header.purchasing_unit,
            "Publication date": header.publication_date,
            "Item No.": lot_number,
            "Basic productive incentive": "YES" if str(basic_prod).lower() in ("true", "1", "yes") else "NO",
        })

        # Fetch award results per item (can be multiple suppliers/rows)
        results = []
        if item_no is not None:
            results = fetch_results_for_item(client, orgao, ano, compra, int(item_no))

        if not results:
            # no award rows; still output the item row
            out_rows.append(base)
            continue

        # One output row per award result
        for idx, r in enumerate(results, start=1):
            row = dict(base)

            bidder = _as_str(_safe_get(r, "nomeRazaoSocialFornecedor", "nomeFornecedor", "fornecedorNome"))
            ni_fornecedor = _as_str(_safe_get(r, "niFornecedor", "cnpjFornecedor", "cpfCnpjFornecedor"))

            awarded_qty = _safe_get(r, "quantidadeHomologada", "quantidade", "qtdHomologada")
            awarded_unit = _safe_get(r, "valorUnitarioHomologado", "valorUnitario", "precoUnitario")
            awarded_total = _safe_get(r, "valorTotalHomologado", "valorTotal", "valorTotalAdjudicado")

            award_date = _parse_iso_date(_safe_get(r, "dataResultado", "dataInclusao", "dataHomologacao", "dataAprovacao"))

            # Extract evaluation scores from award result
            # These might not be in the results endpoint, may need to use ratios from purchase
            price_eval = _as_str(_safe_get(r, "notaPreco", "avaliacaoPreco", "scorePreco", "notaValor"))
            quality_eval = _as_str(_safe_get(r, "notaQualidade", "avaliacaoQualidade", "scoreQualidade", "notaTecnica"))
            other_eval = _as_str(_safe_get(r, "notaOutros", "avaliacaoOutros", "scoreOutros"))
            
            # If evaluation scores not found, use ratios (common when only price evaluation exists)
            if not price_eval and price_eval_ratio:
                price_eval = price_eval_ratio
            if not quality_eval and quality_eval_ratio:
                quality_eval = quality_eval_ratio
            if not other_eval and other_eval_ratio:
                other_eval = other_eval_ratio

            # Bid status: if has result row, treat as YES
            row["Bid Status Award"] = "YES"
            row["Bidder"] = bidder
            row["Supplier Identification Number"] = ni_fornecedor
            row["Award Date"] = award_date
            row["Awarded Quantity"] = _as_str(awarded_qty)
            row["Awarded Unit Price"] = _as_str(awarded_unit)
            row["Lot_Award_Value_Local"] = _as_str(awarded_total)
            row["Ranking Order"] = _as_str(_safe_get(r, "ordemClassificacaoSrp", "ordemClassificacao", "ranking", "posicao", "ordem")) or str(idx)
            row["Price Evaluation"] = price_eval
            row["Quality Evaluation"] = quality_eval
            row["Other Evaluation"] = other_eval

            # In UI, Awarded Lot Title often equals item description; keep same unless API provides different
            row["Awarded Lot Title"] = _as_str(_safe_get(r, "descricaoItem", "descricao", "itemDescricao")) or desc

            # Your sample repeats notice link for award link as well
            row["Original_Publication_Link_Award"] = header.publication_link_notice

            out_rows.append(row)

    return out_rows


def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        w.writeheader()
        for r in rows:
            # keep columns stable
            w.writerow({k: r.get(k, "") for k in OUTPUT_COLUMNS})


def process_single_cn(client: PNCPClient, cn_number: str) -> List[Dict[str, Any]]:
    """
    Process a single CN number and return rows.
    """
    try:
        orgao, ano, compra = parse_cn_number(cn_number)
        notice_url = build_editais_url(orgao, ano, compra)
        
        print(f"[Processing] CN: {cn_number} -> URL: {notice_url}")
        
        header = fetch_header(client, orgao, ano, compra, notice_url, cn_number)
        
        # Fetch purchase JSON for evaluation ratios and MEAT
        purchase_json = None
        try:
            purchase_json = client.get_json(api_purchase_url(orgao, ano, compra))
        except Exception:
            pass  # Will use None, which is fine
        
        items = fetch_items(client, orgao, ano, compra)
        
        rows = build_rows(header, items, client, orgao, ano, compra, purchase_json)
        
        print(f"[OK] CN {cn_number}: {len(rows)} rows")
        return rows
    except Exception as e:
        print(f"[ERROR] Failed to process CN {cn_number}: {e}")
        import traceback
        traceback.print_exc()
        return []


def main() -> int:
    script_dir = Path(__file__).parent
    output_csv = script_dir / "output.csv"

    # Allow override via command line arguments
    # Usage:  python GetData.py [input_csv] [output_csv]
    #   or:   python GetData.py [output_csv]   (when DAYS > 0)
    if DAYS == 0:
        # Mode: read CN numbers from Input.csv
        input_csv = script_dir / "Input.csv"
        if len(sys.argv) >= 2:
            input_csv = Path(sys.argv[1])
        if len(sys.argv) >= 3:
            output_csv = Path(sys.argv[2])

        cn_numbers = read_cn_numbers_from_csv(str(input_csv))
        print(f"[INFO] Found {len(cn_numbers)} CN numbers in {input_csv}")
        
        # If input list is empty, fetch ALL tenders for last 7 days
        if not cn_numbers:
            print("[INFO] Input list is empty → fetching ALL tenders for last 7 days")
            cn_numbers = fetch_cn_numbers_from_api(7)
    else:
        # Mode: fetch CN numbers from PNCP API for last N days
        if len(sys.argv) >= 2:
            output_csv = Path(sys.argv[1])

        # Check if SearchTerm.csv exists → use search API, otherwise use modality-based API
        search_csv = script_dir / "SearchTerm.csv"
        if search_csv.exists():
            print(f"[INFO] SearchTerm.csv detected → using keyword-based search API")
            cn_numbers = fetch_cn_numbers_from_search_api(DAYS)
        else:
            print(f"[INFO] SearchTerm.csv not found → using modality-based consulta API")
            cn_numbers = fetch_cn_numbers_from_api(DAYS)

    if not cn_numbers:
        print("[ERROR] No CN numbers found")
        return 1

    # Process all CN numbers
    client = PNCPClient()
    all_rows = []

    for i, cn_number in enumerate(cn_numbers, 1):
        print(f"\n--- [{i}/{len(cn_numbers)}] ---")
        rows = process_single_cn(client, cn_number)
        all_rows.extend(rows)

    # Write combined output
    write_csv(str(output_csv), all_rows)
    print(f"\n[OK] Total: Wrote {len(all_rows)} rows -> {output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
