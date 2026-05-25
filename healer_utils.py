import os
import re
import sqlite3
import zipfile
import shutil
from bs4 import BeautifulSoup

from lncrawl.core.app import App
from lncrawl.core.sources import load_sources

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
    # CHANGED: We now store chapter_title instead of a useless ID
    c.execute('''CREATE TABLE IF NOT EXISTS chapters (novel_url TEXT, chapter_index INTEGER, chapter_title TEXT)''')
    conn.commit()
    return conn

def get_db_toc_count(url):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM chapters WHERE novel_url=?", (url,))
    count = c.fetchone()[0]
    conn.close()
    return count

def normalize_title(t):
    """Strips all spaces, punctuation, and casing for bulletproof matching."""
    return re.sub(r'\W+', '', str(t)).lower()

def scrape_toc_worker(url):
    app = App()
    try:
        app.user_input = url
        app.prepare_search() 
        app.get_novel_info()
        
        chapters = []
        for idx, chap in enumerate(app.crawler.chapters):
            # Extract the actual string title of the chapter
            chap_title = chap.get('title', '') if isinstance(chap, dict) else getattr(chap, 'title', '')
            chapters.append((url, idx, str(chap_title)))
            
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
    
    # --- FETCH CANONICAL TOC MAP FROM DATABASE ---
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT chapter_index, chapter_title FROM chapters WHERE novel_url=?", (source_url,))
    db_rows = c.fetchall()
    conn.close()

    if not db_rows:
        shutil.rmtree(extract_dir, ignore_errors=True)
        return "ERROR", f"URL not found in DB: {source_url}"

    # Build a lookup dictionary: normalized_title -> true_index
    canonical_toc = {normalize_title(title): idx for idx, title in db_rows}
    
    epub_chapters = [f for r, d, fs in os.walk(extract_dir) for f in fs if f.startswith("chapter_") and f.endswith(".xhtml")]

    if len(epub_chapters) < len(db_rows):
        shutil.rmtree(extract_dir, ignore_errors=True)
        return "MISSING", source_url  

    opf_path = next((os.path.join(r, f) for r, _, fs in os.walk(extract_dir) for f in fs if f.endswith(".opf")), None)
    with open(opf_path, 'r', encoding='utf-8') as f:
        opf_soup = BeautifulSoup(f.read(), 'xml')
        
    manifest = opf_soup.find('manifest')
    id_to_href = {item.get('id'): item.get('href') for item in manifest.find_all('item') if item.get('id')}

    # --- MAP EPUB FILES TO THEIR TRUE INDEX VIA TITLE ---
    file_to_true_index = {}
    for chap_file in epub_chapters:
        abs_chap_path = next((os.path.join(r, chap_file) for r, _, fs in os.walk(extract_dir) if chap_file in fs), None)
        with open(abs_chap_path, 'r', encoding='utf-8') as f:
            chap_soup = BeautifulSoup(f.read(), 'html.parser')
            
        # lncrawl puts the title in <title> and usually <h1> or <h3>
        title_tag = chap_soup.find('title')
        h1_tag = chap_soup.find('h1')
        
        chap_title = ""
        if title_tag and title_tag.text.strip():
            chap_title = title_tag.text.strip()
        elif h1_tag and h1_tag.text.strip():
            chap_title = h1_tag.text.strip()
            
        norm = normalize_title(chap_title)
        
        if norm in canonical_toc:
            file_to_true_index[chap_file] = canonical_toc[norm]
        else:
            # Fallback if title is inexplicably garbled
            fallback_num = int(re.search(r'\d+', chap_file).group()) if re.search(r'\d+', chap_file) else 0
            file_to_true_index[chap_file] = 999999 + fallback_num

    # --- REORDER THE SPINE ---
    spine = opf_soup.find('spine')
    itemrefs = spine.find_all('itemref')
    
    def sort_key(tag):
        idref = tag.get('idref')
        href = id_to_href.get(idref)
        if href and href in file_to_true_index:
            return (1, file_to_true_index[href])
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
