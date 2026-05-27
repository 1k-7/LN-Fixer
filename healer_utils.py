import os
import re
import zipfile
import shutil
import requests
import html
import logging
from urllib.parse import urlparse, parse_qs
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

from lncrawl.core.app import App
from lncrawl.core.sources import load_sources

logger = logging.getLogger(__name__)

# --- PURE REQUESTS CONNECTION POOL ---
SHARED_SESSION = requests.Session()
SHARED_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
})

_old_session_request = requests.Session.request
def _new_session_request(self, method, url, **kwargs):
    if kwargs.get('timeout') is None:
        kwargs['timeout'] = 20.0
    return _old_session_request(self, method, url, **kwargs)
requests.Session.request = _new_session_request

adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=1)
SHARED_SESSION.mount('http://', adapter)
SHARED_SESSION.mount('https://', adapter)
# -------------------------------------

# ==============================================================================
# 🚨 RUNTIME MONKEY-PATCH: FIXING THE ROOT CAUSE 🚨
# We override the FanMTL scraper in memory so it fetches pages STRICTLY sequentially.
# This guarantees 1:1 native source order without any mathematical guesswork.
# ==============================================================================
try:
    from lncrawl.sources.en.f.fanmtl import FanMTLCrawler

    def custom_fanmtl_initialize(self):
        self.init_executor(3)
        self.scraper = requests.Session()
        self.scraper.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        })
        self.cleaner.bad_css.update({'div[align="center"]'})
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(pool_connections=4, pool_maxsize=4, max_retries=retry)
        self.scraper.mount("https://", adapter)
        self.scraper.mount("http://", adapter)

    def custom_fanmtl_read_novel_info(self):
        logger.info("Using Runtime-Patched FanMTL Scraper (STRICT 1:1 ORDER)")
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

        # 1. Parse initial soup (Page 1) FIRST to lock the first chapters into position.
        self.parse_chapter_list(soup)

        # 2. Safely extract all other valid pages
        pagination_links = soup.select('.pagination a[data-ajax-update="#chpagedlist"]')
        if pagination_links:
            common_url = self.novel_url
            wjm = ""
            pages_to_fetch = set()
            
            for link in pagination_links:
                href = link.get("href")
                if href and "?" in href:
                    base, query_str = href.split("?", 1)
                    common_url = self.absolute_url(base)
                    query = parse_qs(query_str)
                    if "page" in query:
                        try:
                            p = int(query["page"][0])
                            if p > 1: # Skip page 1, we already have it from the initial soup
                                pages_to_fetch.add(p)
                        except: pass
                    if "wjm" in query:
                        wjm = query["wjm"][0]

            if pages_to_fetch:
                sorted_pages = sorted(list(pages_to_fetch))
                futures = {}
                
                # Submit requests asynchronously for download speed
                for page in sorted_pages:
                    url = f"{common_url}?page={page}&wjm={wjm}"
                    futures[page] = self.executor.submit(self.get_soup, url)

                # BUT parse the DOM strictly sequentially (Page 2 -> 3 -> 4...)
                for page in sorted_pages:
                    try:
                        page_soup = futures[page].result() # Block and wait for exact sequence
                        self.parse_chapter_list(page_soup)
                    except Exception as e:
                        logger.error(f"Failed to fetch FanMTL page {page}: {e}")

    FanMTLCrawler.initialize = custom_fanmtl_initialize
    FanMTLCrawler.read_novel_info = custom_fanmtl_read_novel_info
except ImportError:
    logger.warning("FanMTLCrawler not found. Skipping monkey-patch.")
# ==============================================================================

def clean_text(text):
    if text is None: return ""
    decoded = html.unescape(str(text))
    return " ".join(decoded.split())

def extract_url_from_epub(epub_path):
    extract_dir = epub_path + "_unzipped"
    os.makedirs(extract_dir, exist_ok=True)
    
    try:
        with zipfile.ZipFile(epub_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
    except zipfile.BadZipFile:
        shutil.rmtree(extract_dir, ignore_errors=True)
        return None, extract_dir, "Bad Zip File"

    intro_path = next((os.path.join(r, f) for r, _, fs in os.walk(extract_dir) for f in fs if f == "intro.xhtml"), None)
    if not intro_path:
        shutil.rmtree(extract_dir, ignore_errors=True)
        return None, extract_dir, "No intro.xhtml found"

    with open(intro_path, 'r', encoding='utf-8') as f:
        match = re.search(r'Source:</b>\s*<a href="([^"]+)">', f.read())
        
    if not match:
        shutil.rmtree(extract_dir, ignore_errors=True)
        return None, extract_dir, "Source URL not found inside EPUB"
        
    return match.group(1), extract_dir, None

def fetch_live_toc(url):
    """
    Fetches TOC exactly as it appears on the source IRL.
    ZERO mathematical sorting. ZERO guesswork.
    """
    app = App()
    try:
        app.user_input = url
        app.prepare_search() 
        
        if app.crawler:
            app.crawler.scraper = SHARED_SESSION
            
        app.get_novel_info()
        
        canonical_toc = {}
        has_duplicates = False
        
        for idx, chap in enumerate(app.crawler.chapters):
            chap_title = chap.get('title', '') if isinstance(chap, dict) else getattr(chap, 'title', '')
            cleaned = clean_text(chap_title)
            
            if cleaned:
                if cleaned in canonical_toc:
                    has_duplicates = True
                canonical_toc[cleaned] = idx
                
        return canonical_toc, has_duplicates, None
    except Exception as e:
        return None, False, str(e)
    finally:
        app.destroy()

def extract_title_from_html(html_path):
    with open(html_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
    title_tag = soup.find('title')
    return title_tag.text if title_tag else ""

def fix_epub_spine(epub_path, extract_dir, canonical_toc, log_data):
    epub_chapters = [f for r, _, fs in os.walk(extract_dir) for f in fs if f.startswith("chapter_") and f.endswith(".xhtml")]

    if len(epub_chapters) < len(canonical_toc):
        shutil.rmtree(extract_dir, ignore_errors=True)
        log_data.append("❌ Missing chapters detected in EPUB compared to live source.")
        return "REDOWNLOAD", None

    file_to_true_index = {}
    seen_epub_titles = set()
    
    for chap_file in epub_chapters:
        abs_chap_path = next((os.path.join(r, chap_file) for r, _, fs in os.walk(extract_dir) if chap_file in fs), None)
        raw_title = extract_title_from_html(abs_chap_path)
        cleaned_title = clean_text(raw_title)
        
        if cleaned_title in seen_epub_titles:
            shutil.rmtree(extract_dir, ignore_errors=True)
            log_data.append(f"⚠️ Duplicate title found inside EPUB: '{raw_title}'. Cannot safely sort.")
            return "REDOWNLOAD", None
        seen_epub_titles.add(cleaned_title)
        
        if cleaned_title in canonical_toc:
            file_to_true_index[chap_file] = canonical_toc[cleaned_title]
        else:
            shutil.rmtree(extract_dir, ignore_errors=True)
            log_data.append(f"⚠️ Exact title '{raw_title}' not found in live source TOC. Source may have changed.")
            return "REDOWNLOAD", None

    # --- 1. REORDER OPF ---
    opf_path = next((os.path.join(r, f) for r, _, fs in os.walk(extract_dir) for f in fs if f.endswith(".opf")), None)
    with open(opf_path, 'r', encoding='utf-8') as f:
        opf_soup = BeautifulSoup(f.read(), 'xml')
        
    manifest = opf_soup.find('manifest')
    id_to_href = {item.get('id'): item.get('href') for item in manifest.find_all('item') if item.get('id')}

    spine = opf_soup.find('spine')
    itemrefs = spine.find_all('itemref')
    
    def sort_key(tag):
        idref = tag.get('idref')
        href = id_to_href.get(idref)
        if href:
            basename = os.path.basename(href.split('#')[0])
            if basename in file_to_true_index: 
                return (1, file_to_true_index[basename])
                
        if idref and idref.startswith('volume_'): 
            vol_num = int(re.search(r'\d+', idref).group()) if re.search(r'\d+', idref) else 0
            return (0, vol_num)
            
        return (-1, 0)

    sorted_itemrefs = sorted(itemrefs, key=sort_key)
    
    if itemrefs == sorted_itemrefs:
        shutil.rmtree(extract_dir, ignore_errors=True)
        log_data.append("✅ EPUB is perfectly synced with the live source. No changes needed.")
        return "OK", None

    spine.clear()
    for item in sorted_itemrefs: spine.append(item)

    with open(opf_path, 'w', encoding='utf-8') as f:
        f.write(str(opf_soup))

    # --- 2. REORDER NCX ---
    ncx_path = next((os.path.join(r, f) for r, _, fs in os.walk(extract_dir) for f in fs if f.endswith(".ncx")), None)
    if ncx_path:
        with open(ncx_path, 'r', encoding='utf-8') as f:
            ncx_soup = BeautifulSoup(f.read(), 'xml')
        
        navmap = ncx_soup.find('navMap')
        if navmap:
            navpoints = navmap.find_all('navPoint', recursive=False)
            
            def ncx_sort(tag):
                content = tag.find('content')
                src = content.get('src') if content else ''
                basename = os.path.basename(src.split('#')[0])
                return file_to_true_index.get(basename, 999999)
                
            sorted_navs = sorted(navpoints, key=ncx_sort)
            navmap.clear()
            for i, nav in enumerate(sorted_navs):
                nav['playOrder'] = str(i + 1)
                navmap.append(nav)
                
            with open(ncx_path, 'w', encoding='utf-8') as f:
                f.write(str(ncx_soup))

    # --- 3. REORDER TOC.XHTML ---
    toc_xhtml = next((os.path.join(r, f) for r, _, fs in os.walk(extract_dir) for f in fs if f in ["toc.xhtml", "nav.xhtml"]), None)
    if toc_xhtml:
        with open(toc_xhtml, 'r', encoding='utf-8') as f:
            toc_soup = BeautifulSoup(f.read(), 'html.parser')
        
        nav_list = toc_soup.find(['ol', 'ul'])
        if nav_list:
            items = nav_list.find_all('li', recursive=False)
            
            def toc_sort(tag):
                a_tag = tag.find('a')
                href = a_tag.get('href') if a_tag else ''
                basename = os.path.basename(href.split('#')[0])
                return file_to_true_index.get(basename, 999999)
                
            sorted_items = sorted(items, key=toc_sort)
            nav_list.clear()
            for item in sorted_items:
                nav_list.append(item)
                
            with open(toc_xhtml, 'w', encoding='utf-8') as f:
                f.write(str(toc_soup))
        
    fixed_epub_path = epub_path.replace('.epub', '_fixed.epub')
    with zipfile.ZipFile(fixed_epub_path, 'w') as zipf:
        mimetype_path = os.path.join(extract_dir, 'mimetype')
        if os.path.exists(mimetype_path):
            zipf.write(mimetype_path, 'mimetype', compress_type=zipfile.ZIP_STORED)
            
        for r, _, fs in os.walk(extract_dir):
            for file in fs:
                if file == 'mimetype' and r == extract_dir:
                    continue 
                abs_path = os.path.join(r, file)
                zipf.write(abs_path, os.path.relpath(abs_path, extract_dir), compress_type=zipfile.ZIP_DEFLATED)
                
    shutil.rmtree(extract_dir, ignore_errors=True)
    log_data.append(f"🛠️ Successfully mapped {len(epub_chapters)} chapters word-for-word. All Spines and TOCs rewritten.")
    return "FIXED", fixed_epub_path

def redownload_worker(url, out_dir):
    load_sources()
    app = App()
    try:
        app.user_input = url
        app.output_path = out_dir
        app.pack_by_volume = False
        app.output_formats = {'epub': True}
        app.prepare_search()
        app.get_novel_info()
        for _ in app.start_download(): pass
        for fmt, f in app.bind_books(): return f
        return None
    except Exception as e:
        return None
    finally:
        app.destroy()