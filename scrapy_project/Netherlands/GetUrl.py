from __future__ import annotations

from urllib.parse import urlencode
import scrapy


class DadasdaSpider(scrapy.Spider):
    """
    End-to-end Scrapy spider for Medicijnkosten infinite-scroll search results.

    It loads:
      1) the human search page (to read total from #summary and get first batch)
      2) then calls the XHR endpoint:
         /zoeken?page=N&searchTerm=...&sorting=&debugMode=
         with header X-Requested-With: XMLHttpRequest

    Output:
      medicijnkosten_links.jsonl  (one {"url": "..."} per unique /medicijn link)
    """

    name = "dadasda"
    allowed_domains = ["www.medicijnkosten.nl"]

    # Your original "human" page with full filters (referer)
    HUMAN_URL = (
        "https://www.medicijnkosten.nl/zoeken"
        "?searchTerm=632%20Medicijnkosten%20Drugs4"
        "&type=medicine"
        "&searchTermHandover=632%20Medicijnkosten%20Drugs4"
        "&vorm=VLOEISTOF"
        "&sterkte=Alle%20sterktes"
    )

    # The XHR endpoint only needs these params (as seen in DevTools)
    SEARCH_TERM_XHR = "632 Medicijnkosten Drugs4"

    custom_settings = {
        # ---------- IMPORTANT: disable your platform stack for this spider ----------
        # Disable DB pipelines (your logs showed SQLite/Postgres pipeline writing 0 items)
        "ITEM_PIPELINES": {},

        # Disable your custom downloader middlewares that can redirect/proxy/block
        "DOWNLOADER_MIDDLEWARES": {
            "pharma.middlewares.OTelDownloaderMiddleware": None,
            "pharma.middlewares.PlatformDBLoggingMiddleware": None,
            "pharma.middlewares.RandomUserAgentMiddleware": None,
            "pharma.middlewares.BrowserHeadersMiddleware": None,
            "pharma.middlewares.HumanizeDownloaderMiddleware": None,
            "pharma.middlewares.AntiBotDownloaderMiddleware": None,
            "pharma.middlewares.ProxyRotationMiddleware": None,

            # Keep Scrapy basics
            "scrapy.downloadermiddlewares.offsite.OffsiteMiddleware": 50,
            "scrapy.downloadermiddlewares.httpauth.HttpAuthMiddleware": 300,
            "scrapy.downloadermiddlewares.downloadtimeout.DownloadTimeoutMiddleware": 350,
            "scrapy.downloadermiddlewares.defaultheaders.DefaultHeadersMiddleware": 400,
            "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": 500,
            "scrapy.downloadermiddlewares.retry.RetryMiddleware": 550,
            "scrapy.downloadermiddlewares.redirect.MetaRefreshMiddleware": 580,
            "scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware": 590,
            "scrapy.downloadermiddlewares.redirect.RedirectMiddleware": 600,
            "scrapy.downloadermiddlewares.cookies.CookiesMiddleware": 700,
            "scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware": 750,
            "scrapy.downloadermiddlewares.stats.DownloaderStats": 850,
        },

        # ---------- Crawl safety ----------
        "ROBOTSTXT_OBEY": True,
        "COOKIES_ENABLED": True,

        "RETRY_ENABLED": True,
        "RETRY_TIMES": 8,
        "RETRY_HTTP_CODES": [403, 408, 429, 500, 502, 503, 504],
        "DOWNLOAD_TIMEOUT": 45,

        "CONCURRENT_REQUESTS": 4,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_DELAY": 0,

        # ---------- Output ----------
        "FEEDS": {
            "medicijnkosten_links.jsonl": {"format": "jsonlines", "encoding": "utf-8"},
        },

        # ---------- Headers ----------
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html",
            "Accept-Language": "en-US,en;q=0.8",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/144.0.0.0 Safari/537.36"
            ),
        },

        "LOG_LEVEL": "INFO",
        "TELNETCONSOLE_ENABLED": False,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.page = 0
        self.total_expected: int | None = None
        self.seen: set[str] = set()

        self.empty_pages_in_a_row = 0
        self.max_empty_pages = 3  # stop after N pages yielding 0 new links
        self.max_pages_hard_cap = 10000  # safety

    # -----------------------
    # Entry
    # -----------------------
    def start_requests(self):
        # Load the human page first (it contains #summary total + first batch)
        yield scrapy.Request(
            self.HUMAN_URL,
            callback=self.parse_human_page,
            headers={"Referer": self.HUMAN_URL},
            meta={"cookiejar": 1},
            dont_filter=True,
        )

    # -----------------------
    # Parse human page
    # -----------------------
    def parse_human_page(self, response: scrapy.http.Response):
        self.logger.info("HUMAN page status=%s final_url=%s bytes=%s",
                         response.status, response.url, len(response.body))

        # Read total from summary: <strong>1949</strong>
        txt = response.css("#summary h2 strong::text").get()
        if txt and txt.strip().isdigit():
            self.total_expected = int(txt.strip())
            self.logger.info("Total expected from summary: %d", self.total_expected)
        else:
            self.logger.warning("Could not read total from #summary (selector mismatch or blocked page).")

        # Extract links from first page
        new_items = list(self._extract_links(response))
        self.logger.info("HUMAN extracted new links: %d (total_seen=%d)", len(new_items), len(self.seen))
        for it in new_items:
            yield it

        # Now begin XHR pagination from page=1 (human page is effectively page=0)
        self.page = 1
        yield self._make_xhr_request(page=self.page)

    # -----------------------
    # Parse XHR pages
    # -----------------------
    def parse_xhr_page(self, response: scrapy.http.Response):
        self.logger.info("XHR page=%d status=%s bytes=%s url=%s",
                         self.page, response.status, len(response.body), response.url)

        new_items = list(self._extract_links(response))
        new_count = len(new_items)

        self.logger.info("XHR page=%d new_links=%d total_seen=%d",
                         self.page, new_count, len(self.seen))

        for it in new_items:
            yield it

        # Stop if we reached expected total
        if self.total_expected is not None and len(self.seen) >= self.total_expected:
            self.logger.info("Reached total_expected=%d. Stopping.", self.total_expected)
            return

        # Stop if no new links for too long
        if new_count == 0:
            self.empty_pages_in_a_row += 1
        else:
            self.empty_pages_in_a_row = 0

        if self.empty_pages_in_a_row >= self.max_empty_pages:
            self.logger.warning("No new links for %d consecutive pages. Stopping.",
                                self.empty_pages_in_a_row)
            return

        # Next page
        self.page += 1
        if self.page >= self.max_pages_hard_cap:
            self.logger.warning("Hard cap reached (%d). Stopping.", self.max_pages_hard_cap)
            return

        yield self._make_xhr_request(page=self.page)

    # -----------------------
    # Helpers
    # -----------------------
    def _make_xhr_request(self, page: int) -> scrapy.Request:
        # Matches your DevTools request:
        # /zoeken?page=2&searchTerm=632+Medicijnkosten+Drugs4&sorting=&debugMode=&_=...
        # NOTE: the "_" param is cache-buster; not needed.
        params = {
            "page": str(page),
            "searchTerm": self.SEARCH_TERM_XHR,
            "sorting": "",
            "debugMode": "",
        }
        url = "https://www.medicijnkosten.nl/zoeken?" + urlencode(params)

        return scrapy.Request(
            url,
            callback=self.parse_xhr_page,
            headers={
                "Accept": "text/html",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": self.HUMAN_URL,
            },
            meta={"cookiejar": 1},
            dont_filter=True,
        )

    def _extract_links(self, response: scrapy.http.Response):
        """
        Extract all <a class="result-item medicine" href="/medicijn?..."> links.
        Deduplicate by absolute URL to avoid duplicates while never missing.
        """
        hrefs = response.css("a.result-item.medicine::attr(href)").getall()
        self.logger.info("Found %d anchors on %s", len(hrefs), response.url)

        for href in hrefs:
            if not href.startswith("/medicijn?"):
                continue
            abs_url = response.urljoin(href)
            if abs_url in self.seen:
                continue
            self.seen.add(abs_url)
            yield {"url": abs_url}
