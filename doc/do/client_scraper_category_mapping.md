# Scraper Category Mapping (Client Ready)

## Category Definitions

1. **Static Page Scraping**  
   Known/fixed page(s) are fetched and parsed for data.

2. **Search Results Scraping (SERP-style on target site/API)**  
   Automation submits search terms/filters and extracts result lists directly.

3. **PDF / Document Processing**  
   Pipeline processes PDF files and extracts structured data.

4. **Crawling + Scraping (Discover -> Visit -> Extract)**  
   Pipeline discovers IDs/URLs first, then visits each target page/API endpoint and extracts details.

## Scraper-to-Category Mapping

| Scraper | Primary Category | Why |
|---|---|---|
| Argentina | 4. Crawling + Scraping | Builds product list, prepares product URLs, then scrapes product pages and API fallback (`scripts/Argentina/01_getProdList.py`, `scripts/Argentina/02_prepare_urls.py`, `scripts/Argentina/03_alfabeta_selenium_scraper.py`). |
| Belarus | 2. Search Results Scraping | Searches registry by INN and paginates result tables to extract rows (`scripts/Belarus/01_belarus_rceth_extract.py`). |
| Canada Ontario | Mixed: 2 + 1 | Step 1 runs query-based result scraping (`q=a..z`), Step 2 scrapes a fixed EAP page (`scripts/Canada Ontario/01_extract_product_details.py`, `scripts/Canada Ontario/02_ontario_eap_prices.py`). |
| CanadaQuebec | 3. PDF / Document Processing | Splits annex PDFs and extracts structured rows from PDF content (`scripts/CanadaQuebec/01_split_pdf_into_annexes.py`, `scripts/CanadaQuebec/03_extract_annexe_iv1.py`). |
| India | 4. Crawling + Scraping (API-based) | Fetches formulation list, then calls multiple detail APIs per formulation/SKU (`scrapy_project/pharma/spiders/india_details.py`). |
| Italy | 4. Crawling + Scraping (search -> detail API) | Runs broad search queries, then fetches per-item detail payloads (`scripts/Italy/02_scrape_price_reductions_v2.py`). |
| Malaysia | Mixed: 1 + 2 + 4 | MyPriMe/FUKKM are fixed-page scraping; Quest3 does keyword search + CSV + per-product detail visits (`scripts/Malaysia/scrapers/myprime_scraper.py`, `scripts/Malaysia/scrapers/fukkm_scraper.py`, `scripts/Malaysia/scrapers/quest3_scraper.py`). |
| Netherlands | 4. Crawling + Scraping | Collects large URL set first, then scrapes each collected URL (`scripts/Netherlands/scraper.py`). |
| North Macedonia | 4. Crawling + Scraping | Collects detail URLs from registry pages, then scrapes each detail page (`scripts/North Macedonia/01_collect_urls.py`, `scripts/North Macedonia/02_fast_scrape_details.py`). |
| Russia | 1. Static/Paginated Registry Scraping | Applies region filter, iterates paginated registry pages, extracts rows (no separate URL discovery phase) (`scripts/Russia/01_russia_farmcom_scraper.py`, `scripts/Russia/02_russia_farmcom_excluded_scraper.py`). |
| Taiwan | 4. Crawling + Scraping | Collects drug-code URLs first, then visits each URL for detail extraction (`scripts/Taiwan/01_taiwan_collect_drug_code_urls.py.py`, `scripts/Taiwan/02_taiwan_extract_drug_code_details.py`). |
| Tender - Brazil | 4. Crawling + Scraping (API-based) | Uses search/consulta APIs to collect tender IDs, then fetches item and award details (`scripts/Tender - Brazil/GetData.py`). |
| Tender- Chile | 4. Crawling + Scraping | Gets redirect URLs from input tender list, then visits detail/award pages to extract lot-level data (`scripts/Tender- Chile/01_fast_redirect_urls.py`, `scripts/Tender- Chile/02_extract_tender_details.py`, `scripts/Tender- Chile/03_fast_extract_awards.py`). |

## Detailed Flow (PCID Mapping Excluded)

Note: The functional details below intentionally ignore any PCID mapping step.

### Argentina
- Logs in to AlfaBeta and loads full product index (product + company).
- Builds product URLs from index rows.
- Scrapes product detail pages via Selenium worker loops.
- Uses API fallback for unresolved/no-data products.
- Runs dictionary translation and generates final output files.

### Belarus
- Loads INN/generic inputs and searches the RCETH registry.
- Paginates result tables and extracts product/pricing fields.
- Processes/translates extracted Russian text and formats export outputs.

### Canada Ontario
- Runs query-based scraping on Ontario formulary (`q=a..z`) to collect product rows.
- Resolves missing manufacturer data from detail pages when needed.
- Scrapes EAP price page and derives pricing fields.
- Generates final output dataset.

### CanadaQuebec
- Splits source PDF into Annexe IV.1, IV.2, and V sections.
- Optionally validates PDF/table structure.
- Extracts rows from each annexe and normalizes columns.
- Merges annex outputs into final CSV.

### India
- Seeds formulation queue in DB and runs parallel Scrapy workers.
- Calls NPPA APIs for formulation list, SKU tables, MRP, other brands, and medicine details.
- Retries failed/zero-record formulations.
- Runs QC gate and exports final CSV outputs.

### Italy
- Executes broad search query sets against AIFA service.
- Fetches per-result detail payloads for matched items.
- Parses AIC/price values from detail text and writes final JSONL.

### Malaysia
- Step 1 (MyPriMe): opens drug-price page, loads full table, extracts registration rows.
- Step 2 (Quest3): bulk keyword search + CSV ingestion, then individual detail-page fetch for missing records.
- Step 3: consolidates collected detail data.
- Step 4: scrapes FUKKM reimbursable list and attaches reimbursement context.

### Netherlands
- Collects session/cookies, then performs large-scale URL discovery from search results.
- Verifies URL collection completeness against expected totals.
- Visits pending product URLs and extracts detail fields.
- Consolidates extracted records into final table/output.

### North Macedonia
- Crawls registry listing pages to collect detail URLs.
- Scrapes each detail URL (fast HTTP parsing) for drug/pricing fields.
- Runs dictionary translation and validation/stats steps.
- Generates export output.

### Russia
- Scrapes VED registry by region with paginated multi-tab extraction.
- Clicks barcode actions to retrieve EANs and validates row completeness.
- Scrapes excluded list pages in a separate step.
- Retries failed pages, then processes/translates and formats export outputs.

### Taiwan
- Collects drug-code detail URLs from search pages using ATC prefixes.
- Tracks seen/progress and resumes from checkpoints.
- Visits each detail URL and extracts certificate, applicant, and manufacturer data.
- Writes final detail output.

### Tender - Brazil
- Uses PNCP APIs (search/consulta modes) to collect tender control numbers/URLs.
- Fetches tender header, item, and award data per tender.
- Produces one row per item-award combination in final output.

### Tender- Chile
- Reads input tenders and resolves redirect/detail URLs (fast HTTP).
- Extracts tender-level and lot-level details from tender pages.
- Extracts award information from award pages.
- Merges detail + award outputs into final CSV.

## Raw Data -> Calculated Columns (PCID Mapping Excluded)

This section lists only explicit value calculations/derived columns added on top of raw scraped data.

### Canada Ontario
- Raw: `Drug Benefit Price or Unit Price`, `Amount MOH Pays`.
- Derived: `exfactory_price`, `reimbursable_price`, `public_with_vat`, `copay`.
- Rule:
  - `exfactory_price = parsed(DBP column)`
  - `reimbursable_price = parsed(Amount MOH Pays)`; if missing/non-numeric, fallback to `exfactory_price`
  - `public_with_vat = exfactory_price * 1.08`
  - `copay = public_with_vat - reimbursable_price`
- Raw: `local_pack_code`, brand/description text.
- Derived: `price_type`.
- Rule:
  - if pack code ends with `PK` OR description contains token `PK` -> `PACK`
  - else -> `UNIT`

### Netherlands
- Raw: package price with VAT (`ppp_vat`).
- Derived: `ppp_ex_vat`.
- Formula: `ppp_ex_vat = ppp_vat / 1.09`.
- Raw: reimbursement message blocks.
- Derived: `reimbursable_status`, `reimbursable_rate`.
- Rule:
  - success/full message -> `Fully reimbursed`, `100%`
  - warning message -> `Partially reimbursed`
- Raw: warning/deductible text blocks.
- Derived: `copay_price`, `copay_percent`, `deductible`, `ri_with_vat`.
- Rule:
  - parse currency amount from warning/deductible nodes
  - parse `%` from warning text for `copay_percent`
  - if deductible text implies none (`niets`) -> `deductible = 0.0`
  - `ri_with_vat = deductible`
- Fixed derived constants:
  - `vat_percent = 9.0`
  - `currency = EUR`

### Argentina
- Raw: payer fields (`ioma_os`, `ioma_af`, `pami_af`), import status.
- Derived: `RI Source`, `Reimbursement Amount`, `Co-Pay Amount`, `RI Rule Applied`.
- Rule:
  - if IOMA data present -> source `IOMA`, reimbursement = `ioma_os`, copay = `ioma_af`
  - else if PAMI data present -> source `PAMI-only`, reimbursement empty, copay = `pami_af`
  - else if imported flag -> source `IMPORTED`
  - else -> no scheme
- Raw: text money values in mixed locale format.
- Derived: normalized numeric values.
- Rule: locale-aware parser handles `1,234.56`, `1.234,56`, `1234,56`, etc.

### India
- Raw: `pack_size` string (example `30 TABLET`).
- Derived: numeric `PackSize` output.
- Rule: extract leading integer (`30`).
- Raw: `year_month` string (example `Dec-2025`).
- Derived: `YearMonth` output.
- Rule: format `Mon-YYYY` -> `Mon-YY` (example `Dec-25`).
- Raw: `ceiling_price`.
- Derived: `CeilingPrice` output.
- Rule: if empty or `-1`, set to `0`.
- Derived classification column:
  - `BrandType = MAIN` for `in_sku_main` rows
  - `BrandType = OTHER` for `in_brand_alternatives` rows

### Taiwan
- Raw: `Valid Date (ROC)` (Republic of China calendar).
- Derived: `Valid Date (AD)`.
- Formula: `AD year = ROC year + 1911`, output as `YYYY-MM-DD`.

### North Macedonia
- Raw: `formulation`, `packaging`, `strength`, `composition`.
- Derived: `Local Pack Description`.
- Rule: concatenate normalized parts into one description string.
- Fixed business output constants:
  - `Reimbursable Status = PARTIALLY REIMBURSABLE`
  - `Reimbursable Rate = 80.00%`
  - `Copayment Percent = 20.00%`
  - `VAT Percent = 5`

### Tender- Chile
- Raw: evaluation criteria rows containing percentages.
- Derived: `Price Evaluation ratio`, `Quality Evaluation ratio`, `Other Evaluation ratio`.
- Rule:
  - extract `%` from criterion text
  - map known price/quality criteria directly
  - accumulate all remaining criteria into `Other Evaluation ratio`

### Scrapers with little/no numeric custom calculation layer
- `Belarus`, `CanadaQuebec`, `Italy`, `Malaysia`, `Russia`, `Tender - Brazil`
- These primarily do extraction, normalization, translation, validation, and final formatting/merge rather than VAT-style arithmetic columns.

## Not Yet Implemented (Folder Present, No Active Pipeline Files)

- `scripts/Colombia`
- `scripts/Peru`
- `scripts/South Korea`

---

## Email Draft for Client

Subject: Scraper Classification by Data Collection Method

Hi [Client Name],

As requested, we reviewed the scraper repository and classified each scraper by collection method.

We are using the following definitions:
- Static Page Scraping: fixed pages are parsed directly.
- Search Results Scraping: automated search/filtering and extraction of result lists.
- PDF/Document Processing: extraction from PDF documents.
- Crawling + Scraping: discover IDs/URLs first, then visit each target and extract details.

Current classification:
- Argentina: Crawling + Scraping
- Belarus: Search Results Scraping
- Canada Ontario: Mixed (Search Results Scraping + Static Page Scraping)
- CanadaQuebec: PDF/Document Processing
- India: Crawling + Scraping (API-based)
- Italy: Crawling + Scraping (search -> detail API)
- Malaysia: Mixed (Static Page + Search Results + Crawling/Detail extraction)
- Netherlands: Crawling + Scraping
- North Macedonia: Crawling + Scraping
- Russia: Static/Paginated Registry Scraping
- Taiwan: Crawling + Scraping
- Tender Brazil: Crawling + Scraping (API-based)
- Tender Chile: Crawling + Scraping

Additionally, Colombia, Peru, and South Korea currently have placeholder folders but no active scraper pipeline files.

If useful, we can also provide this as:
1) a one-line label per scraper for reporting, and  
2) a technical mapping per pipeline step (Step 1/2/3...) for audit/compliance.

Regards,  
[Your Name]
