import os
import re
import sqlite3
import zipfile
import shutil
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

from lncrawl.core.app import App
from lncrawl.core.sources import load_sources

# --- THE SPEED FIX: PURE REQUESTS CONNECTION POOL ---
# This mimics the exact behavior of chapter scraping. We create ONE session, 
# open 300 TCP sockets, and reuse them for every single request. No Cloudscraper.
SHARED_SESSION = requests.Session()
SHARED_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
})

# Aggressive 15-second timeout killswitch to prevent zombie threads
_old_session_request = requests.Session.request
def _new_session_request(self, method, url, **kwargs):
    if kwargs.get('timeout') is None:
        kwargs['timeout'] = 15.0
    return _old_session_request(self, method, url, **kwargs)
requests.Session.request = _new_session_request

# Mount the massive pool
retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(pool_connections=300, pool_maxsize=300, max_retries=retry)
SHARED_SESSION.mount('http://', adapter)
SHARED_SESSION.mount('https://', adapter)
# ----------------------------------------------------

DB_FILE = "data/tocs.sqlite"

def init_db():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_FILE, timeout=30.0) 
    c = conn.cursor()
    
    c.execute('PRAGMA journal_mode = WAL;')        
    c.execute('PRAGMA synchronous = OFF;')         
    c.execute('PRAGMA cache_size = -1000000;')     
    c.execute('PRAGMA temp_store = MEMORY;')       
    
    c.execute('''CREATE TABLE IF NOT EXISTS novels (url TEXT PRIMARY KEY, title TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS chapters (id TEXT, novel_url TEXT, chapter_index INTEGER)''')
    conn.commit()
    return conn

def get_db_toc_count(url):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM chapters WHERE novel_url=?", (url,))
    count = c.fetchone()[0]
    conn.close()
    return count

def scrape_toc_worker(url):
    app = App()
    try:
        app.user_input = url
        app.prepare_search() 
        
        if app.crawler:
            # THE INJECTION: We overwrite fanmtl's local 4-connection session 
            # with our massive 300-connection global session.
            app.crawler.scraper = SHARED_SESSION
            
        app.get_novel_info()
        
        chapters = []
        for idx, chap in enumerate(app.crawler.chapters):
            chap_id = chap.get('id') if isinstance(chap, dict) else getattr(chap, 'id')
            chapters.append((str(chap_id), url, idx))
            
        return {"url": url, "title": app.crawler.novel_title, "chapters": chapters, "error": None}
    except Exception as e:
        return {"url": url, "error": str(e)}
    finally:
        app.destroy()

def analyze_and_fix_epub(epub_path):
    extract_dir = epub_path + "_unzipped"
    os.makedirs(extract_dir, exist_ok=True)
    
    try:
        with zipfile.ZipFile(epub_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
    except zipfile.BadZipFile:
        shutil.rmtree(extract_dir, ignore_errors=True)
        return "ERROR", "Bad Zip File"

    intro_path = next((os.path.join(r, f) for r, _, fs in os.walk(extract_dir) for f in fs if f == "intro.xhtml"), None)
    if not intro_path:
        shutil.rmtree(extract_dir, ignore_errors=True)
        return "ERROR", "Not an lncrawl generated EPUB (No intro.xhtml)"

    with open(intro_path, 'r', encoding='utf-8') as f:
        match = re.search(r'Source:</b>\s*<a href="([^"]+)">', f.read())
        
    if not match:
        shutil.rmtree(extract_dir, ignore_errors=True)
        return "ERROR", "Source URL not found inside EPUB"
        
    source_url = match.group(1)
    
    db_count = get_db_toc_count(source_url)
    if db_count == 0:
        shutil.rmtree(extract_dir, ignore_errors=True)
        return "ERROR", f"URL not found in DB: {source_url}"
        
    epub_chapters = [f for r, _, fs in os.walk(extract_dir) for f in fs if f.startswith("chapter_") and f.endswith(".xhtml")]

    if len(epub_chapters) < db_count:
        shutil.rmtree(extract_dir, ignore_errors=True)
        return "MISSING", source_url  

    opf_path = next((os.path.join(r, f) for r, _, fs in os.walk(extract_dir) for f in fs if f.endswith(".opf")), None)
    with open(opf_path, 'r', encoding='utf-8') as f:
        opf_soup = BeautifulSoup(f.read(), 'xml')
    
    spine = opf_soup.find('spine')
    itemrefs = spine.find_all('itemref')
    
    chapter_order = [int(re.search(r'\d+', item.get('idref')).group()) 
                     for item in itemrefs if item.get('idref').startswith('chapter_')]

    if chapter_order == sorted(chapter_order):
        shutil.rmtree(extract_dir, ignore_errors=True)
        return "OK", None

    def sort_key(tag):
        idref = tag.get('idref')
        if idref.startswith('chapter_'): return (1, int(re.search(r'\d+', idref).group()))
        if idref.startswith('volume_'): return (0, int(re.search(r'\d+', idref).group()))
        return (-1, 0)

    sorted_itemrefs = sorted(itemrefs, key=sort_key)
    spine.clear()
    for item in sorted_itemrefs: spine.append(item)

    with open(opf_path, 'w', encoding='utf-8') as f:
        f.write(str(opf_soup))
        
    fixed_epub_path = epub_path.replace('.epub', '_fixed.epub')
    with zipfile.ZipFile(fixed_epub_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for r, _, fs in os.walk(extract_dir):
            for file in fs:
                abs_path = os.path.join(r, file)
                zipf.write(abs_path, os.path.relpath(abs_path, extract_dir))
                
    shutil.rmtree(extract_dir, ignore_errors=True)
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
