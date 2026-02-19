
# pncp_scraper.py
# Refactored for database storage

from __future__ import annotations

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

# -- Config loader --
_script_dir = Path(__file__).resolve().parent
_repo_root = _script_dir.parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from core.db.connection import CountryDB
from scripts.tender_brazil.db import BrazilRepository
from config_loader import load_env_file, getenv, getenv_int

load_env_file()

DEFAULT_TIMEOUT = getenv_int("DEFAULT_TIMEOUT", 60)
TOR_PROXIES = None
try:
    from core.network.tor_httpx import TorConfig, setup_tor
    _tor_cfg = TorConfig.from_env(getenv_fn=getenv)
    _tor_proxy_url = setup_tor(_tor_cfg)
    if _tor_proxy_url:
        TOR_PROXIES = {
            "http": _tor_proxy_url.replace("socks5://", "socks5h://"),
            "https": _tor_proxy_url.replace("socks5://", "socks5h://"),
        }
except Exception as e:
    print(f"[WARN] Tor setup failed: {e}")

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

class PNCPClient:
    def __init__(self, session=None):
        self.s = session or requests.Session()
        if TOR_PROXIES: self.s.proxies.update(TOR_PROXIES)
        self.s.headers.update({"User-Agent": "Mozilla/5.0", "Accept": "application/json"})

    def get_json(self, url: str) -> Any:
        r = self.s.get(url, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status()
        return r.json()

def _safe_get(d, *keys):
    for k in keys:
        if k in d and d[k] is not None: return d[k]
    return None

def _as_str(v): return "" if v is None else str(v).strip()

def _parse_iso_date(v):
    if not v: return ""
    s = str(v).strip()
    if re.match(r"^\d{2}-\d{2}-\d{4}$", s): return s
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.strftime("%d-%m-%Y")
    except Exception:
        try:
            dt = datetime.strptime(s[:10], "%Y-%m-%d")
            return dt.strftime("%d-%m-%Y")
        except: return s

def _detect_province(text):
    if not text: return ""
    m = re.search(r"/([A-Z]{2})\b", text.strip())
    return m.group(1) if m else ""

def parse_cn_number(cn):
    match = re.match(r"^(\d+)-1-(\d+)/(\d{4})$", cn.strip())
    if match: return match.groups() # orgao, compra, ano
    raise ValueError(f"Invalid CN: {cn}")

def build_editais_url(orgao, ano, compra): 
    return f"https://pncp.gov.br/app/editais/{orgao}/{ano}/{compra}"

def extract_header_from_api(pj, notice_url, cn):
    h = TenderHeader()
    h.publication_link_notice = notice_url
    h.cn_document_number = cn
    h.source_tender_id = _as_str(_safe_get(pj, "numeroProcesso", "numeroCompra", "numero"))
    local = _as_str(_safe_get(pj, "local", "municipio", "localidade"))
    h.province = _detect_province(local)
    h.tendering_authority = _as_str(_safe_get(pj, "orgaoEntidade", "nomeOrgao"))
    h.purchasing_unit = _as_str(_safe_get(pj, "unidadeCompradora", "nomeUnidadeCompradora"))
    h.tender_title = re.sub(r"\s+", " ", _as_str(_safe_get(pj, "objeto", "descricao")).strip())
    h.type_of_contract = _as_str(_safe_get(pj, "modalidadeNome"))
    h.legal_basis = _as_str(_safe_get(pj, "amparoLegal", "amparoLegalNome"))
    h.budget_source = _as_str(_safe_get(pj, "fonteOrcamentaria", "fonteOrcamentariaNome"))
    h.status = _as_str(_safe_get(pj, "situacaoNome"))
    h.publication_date = _parse_iso_date(_safe_get(pj, "dataPublicacaoPncp"))
    h.deadline_date = _parse_iso_date(_safe_get(pj, "dataFimRecebimentoPropostas"))
    return h

def process_single_cn(client, cn, repo):
    try:
        orgao, ano, compra = parse_cn_number(cn)
        url = build_editais_url(orgao, ano, compra)
        purchase_url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{orgao}/compras/{ano}/{compra}"
        pj = client.get_json(purchase_url)
        header = extract_header_from_api(pj, url, cn)
        
        # Save Header to DB
        repo.insert_tender_details({
            'cn_number': cn,
            'source_tender_id': header.source_tender_id,
            'tender_title': header.tender_title,
            'province': header.province,
            'authority': header.tendering_authority,
            'purchasing_unit': header.purchasing_unit,
            'status': header.status,
            'publication_date': header.publication_date,
            'deadline_date': header.deadline_date,
            'currency': header.local_currency,
            'contract_type': header.type_of_contract,
            'legal_basis': header.legal_basis,
            'budget_source': header.budget_source,
            'notice_link': header.publication_link_notice
        })

        # Process Items & Awards
        items_url = f"{purchase_url}/itens?pagina=1&tamanhoPagina=1000"
        items = client.get_json(items_url)
        if not isinstance(items, list): items = items.get("content", [])

        # MEAT & Ratios
        meat_value = "NO"
        meat = _safe_get(pj, "aplicacaoBeneficioMeEpp", "beneficioMeEpp")
        if meat: meat_value = "YES" if str(meat).lower() in ("true", "1", "yes") else "NO"
        
        price_ratio = ""
        criterios = pj.get("criteriosAvaliacao", [])
        if isinstance(criterios, list):
            for c in criterios:
                if any(k in _as_str(c.get("nome")).upper() for k in ["PREÇO", "PRICE"]):
                    price_ratio = _as_str(c.get("peso"))

        award_rows = []
        for it in items:
            item_no = _safe_get(it, "itemNumber", "numeroItem")
            desc = _as_str(_safe_get(it, "descricao"))
            est_total = _safe_get(it, "valorTotalEstimado")
            est_unit = _safe_get(it, "valorUnitarioEstimado")
            basic_prod = "YES" if str(_safe_get(it, "incentivoProdutivo")).lower() in ("true", "1") else "NO"

            results_url = f"{purchase_url}/itens/{item_no}/resultados"
            try:
                results = client.get_json(results_url)
            except: results = []
            
            if not results:
                award_rows.append({
                    'cn_number': cn, 'item_no': str(item_no), 'lot_number': str(item_no), 'lot_title': desc,
                    'est_lot_value_local': est_total, 'ceiling_unit_price': est_unit, 'meat': meat_value,
                    'price_eval_ratio': price_ratio, 'basic_productive_incentive': basic_prod
                })
            else:
                for r in results:
                    award_rows.append({
                        'cn_number': cn, 'item_no': str(item_no), 'lot_number': str(item_no), 'lot_title': desc,
                        'awarded_lot_title': _as_str(r.get("itemDescricao")) or desc,
                        'est_lot_value_local': est_total, 'ceiling_unit_price': est_unit, 'meat': meat_value,
                        'price_eval_ratio': price_ratio, 'basic_productive_incentive': basic_prod,
                        'bidder': _as_str(r.get("nomeRazaoSocialFornecedor")),
                        'bidder_id': _as_str(r.get("niFornecedor")),
                        'bid_status': "YES", 'award_date': _parse_iso_date(r.get("dataResultado")),
                        'awarded_qty': r.get("quantidadeHomologada"),
                        'awarded_unit_price': r.get("valorUnitarioHomologado"),
                        'lot_award_value_local': r.get("valorTotalHomologado"),
                        'ranking_order': _as_str(r.get("ordemClassificacao"))
                    })
        
        if award_rows:
            repo.insert_tender_awards_bulk(award_rows)
        
        print(f"[OK] {cn}: {len(award_rows)} award rows saved")
        repo.mark_progress(2, "Fetch Details", cn, "completed")
    except Exception as e:
        print(f"[ERROR] {cn}: {e}")
        repo.mark_progress(2, "Fetch Details", cn, "failed", str(e))

def main():
    run_id = os.environ.get("BRAZIL_RUN_ID")
    if not run_id:
        print("[ERROR] BRAZIL_RUN_ID not found")
        sys.exit(1)

    db = CountryDB("Tender_Brazil")
    repo = BrazilRepository(db, run_id)
    cn_numbers = repo.get_tender_list()
    completed = repo.get_completed_keys(2)

    client = PNCPClient()
    for cn in cn_numbers:
        if cn in completed: continue
        process_single_cn(client, cn, repo)
    
    db.close()
    print("[DONE] Brazil Details Fetch complete.")

if __name__ == "__main__":
    main()
