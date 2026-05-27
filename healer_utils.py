import os
import re
import zipfile
import shutil
import requests
import html
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

def clean_text(text):
    """Word-for-Word mapping: Unescapes HTML and standardizes spacing."""
    if text is None: return ""
    decoded = html.unescape(str(text))
    return " ".join(decoded.split())

def extract_anchor_number(title):
    """Safely extracts the primary identifying chapter number."""
    t = str(title).lower()
    # Lock explicit chapter designators first
    match = re.search(r'(?:chapter|ch\.?|c)\s*(\d+(?:\.\d+)?)', t)
    if match: return float(match.group(1))
    
    # Fallback to the first available number
    nums = re.findall(r'\d+', t)
    if nums: return float(nums[0])
    
    return None

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
    """Fetches TOC and uses Chunk Boundary Sorting to fix async bugs without destroying Interludes."""
    app = App()
    try:
        app.user_input = url
        app.prepare_search() 
        
        if app.crawler:
            app.crawler.scraper = SHARED_SESSION
            
        app.get_novel_info()
        
        raw_chapters = []
        for chap in app.crawler.chapters:
            chap_title = chap.get('title', '') if isinstance(chap, dict) else getattr(chap, 'title', '')
            raw_chapters.append(chap_title)
            
        # --- CHUNK BOUNDARY SORTING ALGORITHM ---
        chunks = []
        current_chunk = []
        last_num = None
        
        for title in raw_chapters:
            num = extract_anchor_number(title)
            
            if num is not None and last_num is not None:
                # If chapter jumps heavily forward or backward, it's a page boundary
                if abs(num - last_num) > 25: 
                    chunks.append(current_chunk)
                    current_chunk = []
            
            current_chunk.append(title)
            if num is not None:
                last_num = num
                
        if current_chunk:
            chunks.append(current_chunk)
            
        def chunk_sort_key(chunk):
            """Sorts the chunk based on its first numbered chapter"""
            for title in chunk:
                num = extract_anchor_number(title)
                if num is not None: return num
            return 999999
            
        chunks.sort(key=chunk_sort_key)
        sorted_raw_chapters = [title for chunk in chunks for title in chunk]
        # ----------------------------------------
        
        canonical_toc = {}
        has_duplicates = False
        
        for idx, chap_title in enumerate(sorted_raw_chapters):
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
        # STRICT EPUB STANDARD: mimetype must be first and uncompressed
        mimetype_path = os.path.join(extract_dir, 'mimetype')
        if os.path.exists(mimetype_path):
            zipf.write(mimetype_path, 'mimetype', compress_type=zipfile.ZIP_STORED)
            
        for r, _, fs in os.walk(extract_dir):
            for file in fs:
                if file == 'mimetype' and r == extract_dir:
                    continue # Already wrote this
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