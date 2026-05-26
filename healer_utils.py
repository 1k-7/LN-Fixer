import os
import re
import zipfile
import shutil
import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup

from lncrawl.core.app import App
from lncrawl.core.sources import load_sources

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

def normalize_title(t):
    """Strips spaces, punctuation, and casing for bulletproof title matching."""
    return re.sub(r'\W+', '', str(t)).lower()

def extract_url_from_epub(epub_path):
    """Unzips EPUB, reads intro.xhtml, returns URL, and leaves folder open for processing."""
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
    """Scrapes the website on-the-fly for the canonical TOC."""
    app = App()
    try:
        app.user_input = url
        app.prepare_search() 
        
        if app.crawler:
            app.crawler.scraper = SHARED_SESSION
            
        app.get_novel_info()
        
        canonical_toc = {}
        for idx, chap in enumerate(app.crawler.chapters):
            chap_title = chap.get('title', '') if isinstance(chap, dict) else getattr(chap, 'title', '')
            norm = normalize_title(chap_title)
            if norm:
                canonical_toc[norm] = idx
                
        return canonical_toc, None
    except Exception as e:
        return None, str(e)
    finally:
        app.destroy()

def fix_epub_spine(epub_path, extract_dir, canonical_toc):
    """Reads HTML titles, maps to canonical TOC, reorders spine, zips it up."""
    epub_chapters = [f for r, _, fs in os.walk(extract_dir) for f in fs if f.startswith("chapter_") and f.endswith(".xhtml")]

    if len(epub_chapters) < len(canonical_toc):
        shutil.rmtree(extract_dir, ignore_errors=True)
        return "MISSING", None

    opf_path = next((os.path.join(r, f) for r, _, fs in os.walk(extract_dir) for f in fs if f.endswith(".opf")), None)
    with open(opf_path, 'r', encoding='utf-8') as f:
        opf_soup = BeautifulSoup(f.read(), 'xml')
        
    manifest = opf_soup.find('manifest')
    id_to_href = {item.get('id'): item.get('href') for item in manifest.find_all('item') if item.get('id')}

    # Match EPUB files to true index via Title
    file_to_true_index = {}
    for chap_file in epub_chapters:
        abs_chap_path = next((os.path.join(r, chap_file) for r, _, fs in os.walk(extract_dir) if chap_file in fs), None)
        with open(abs_chap_path, 'r', encoding='utf-8') as f:
            chap_soup = BeautifulSoup(f.read(), 'html.parser')
            
        title_tag = chap_soup.find('title')
        h1_tag = chap_soup.find('h1')
        
        chap_title = ""
        if title_tag and title_tag.text.strip(): chap_title = title_tag.text.strip()
        elif h1_tag and h1_tag.text.strip(): chap_title = h1_tag.text.strip()
            
        norm = normalize_title(chap_title)
        
        if norm in canonical_toc:
            file_to_true_index[chap_file] = canonical_toc[norm]
        else:
            fallback_num = int(re.search(r'\d+', chap_file).group()) if re.search(r'\d+', chap_file) else 0
            file_to_true_index[chap_file] = 999999 + fallback_num

    # Reorder Spine
    spine = opf_soup.find('spine')
    itemrefs = spine.find_all('itemref')
    
    def sort_key(tag):
        idref = tag.get('idref')
        href = id_to_href.get(idref)
        if href and href in file_to_true_index: return (1, file_to_true_index[href])
        if idref and idref.startswith('volume_'): 
            vol_num = int(re.search(r'\d+', idref).group()) if re.search(r'\d+', idref) else 0
            return (0, vol_num)
        return (-1, 0)

    sorted_itemrefs = sorted(itemrefs, key=sort_key)

    if itemrefs == sorted_itemrefs:
        shutil.rmtree(extract_dir, ignore_errors=True)
        return "OK", None

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
