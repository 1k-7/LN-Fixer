# -*- coding: utf-8 -*-
import logging
import requests
from urllib.parse import urlparse, parse_qs
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from lncrawl.models import Chapter
from lncrawl.core.crawler import Crawler

logger = logging.getLogger(__name__)

class FanMTLCrawler(Crawler):
    has_mtl = True
    base_url = "https://www.fanmtl.com/"

    def initialize(self):
        # 1. REDUCE THREADS: 8 is too high for a protected site, keeps RAM lower.
        self.init_executor(3)
        
        # 2. THE FIX: Overwrite the default cloudscraper with a basic Requests Session
        # This prevents it from launching Node.js/JS engines in the background.
        self.scraper = requests.Session()
        
        # 3. Add standard headers so you don't look like a bot
        self.scraper.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        })

        self.cleaner.bad_css.update({'div[align="center"]'})

        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(pool_connections=4, pool_maxsize=4, max_retries=retry)
        self.scraper.mount("https://", adapter)
        self.scraper.mount("http://", adapter)

    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
        # This will now use the lightweight requests session
        soup = self.get_soup(self.novel_url)

        possible_title = soup.select_one("h1.novel-title")
        if possible_title:
            self.novel_title = possible_title.text.strip()
        else:
            meta_title = soup.select_one('meta[property="og:title"]')
            self.novel_title = meta_title.get("content").strip() if meta_title else "Unknown Title"

        img_tag = soup.select_one("figure.cover img") or soup.select_one(".fixed-img img")
        if img_tag:
            url = img_tag.get("src")
            if "placeholder" in str(url) and img_tag.get("data-src"):
                url = img_tag.get("data-src")
            self.novel_cover = self.absolute_url(url)

        author_tag = soup.select_one('.novel-info .author span[itemprop="author"]')
        self.novel_author = author_tag.text.strip() if author_tag else "Unknown"

        summary_div = soup.select_one(".summary .content")
        self.novel_synopsis = summary_div.get_text("\n\n").strip() if summary_div else ""

        self.volumes = [{"id": 1, "title": "Volume 1"}]
        self.chapters = []

        # --- PAGINATION LOGIC ---
        pagination_links = soup.select('.pagination a[data-ajax-update="#chpagedlist"]')
        
        if not pagination_links:
            self.parse_chapter_list(soup)
        else:
            try:
                last_page = pagination_links[-1]
                href = last_page.get("href")
                common_url = self.absolute_url(href).split("?")[0]
                query = parse_qs(urlparse(href).query)
                
                page_params = query.get("page", ["0"])
                page_count = int(page_params[0]) + 1
                
                wjm_params = query.get("wjm", [""])
                wjm = wjm_params[0]

                # Helper to bind the page index to the future result
                def fetch_page(page_idx, p_url):
                    return page_idx, self.get_soup(p_url)

                futures = []
                for page in range(page_count):
                    url = f"{common_url}?page={page}&wjm={wjm}"
                    futures.append(self.executor.submit(fetch_page, page, url))
                
                # Download async, but store in a dictionary to preserve the exact page index
                pages_data = {}
                for result in self.resolve_futures(futures, desc="TOC", unit="page"):
                    page_idx, page_soup = result
                    pages_data[page_idx] = page_soup
                
                # Parse sequentially from Page 0 to Page N. 
                # This guarantees 100% accurate native source order without guesswork.
                for page in range(page_count):
                    if page in pages_data:
                        self.parse_chapter_list(pages_data[page])

            except Exception as e:
                logger.error(f"Pagination failed: {e}. Parsing current page.")
                self.parse_chapter_list(soup)

    def parse_chapter_list(self, soup):
        if not soup: return
        for a in soup.select("ul.chapter-list li a"):
            try:
                self.chapters.append(Chapter(
                    id=len(self.chapters) + 1,
                    volume=1,
                    url=self.absolute_url(a["href"]),
                    title=a.select_one(".chapter-title").text.strip(),
                ))
            except: pass

    def download_chapter_body(self, chapter):
        # Uses the lightweight self.scraper (requests) defined in initialize
        soup = self.get_soup(chapter["url"])
        body = soup.select_one("#chapter-article .chapter-content")
        if not body: return None
        return self.cleaner.extract_contents(body)