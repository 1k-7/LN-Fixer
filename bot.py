import os
import json
import asyncio
import logging
from pyrogram import Client, filters
from pyrogram.types import Message
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

from healer_utils import init_db, scrape_toc_worker, analyze_and_fix_epub, redownload_worker, DB_FILE

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

API_ID = int(os.getenv("API_ID", 123456))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("TELEGRAM_TOKEN", "")

app = Client("healer_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
executor = ProcessPoolExecutor(max_workers=5)
user_states = {}

DATA_DIR = "data"
TEMP_DIR = "temp_epubs"
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)

@app.on_message(filters.command("start"))
async def start_cmd(client, message: Message):
    await message.reply(
        "🛠 **LN EPUB Healer Ready**\n\n"
        "1. Send me your URLs JSON file and reply to it with `/builddb` to build the TOC database.\n"
        "2. Once built, send `/heal` to start the channel processing."
    )

@app.on_message(filters.command("builddb") & filters.reply)
async def builddb_cmd(client, message: Message):
    if not message.reply_to_message.document:
        return await message.reply("Reply to a JSON document!")
        
    status = await message.reply("📥 Downloading JSON...")
    file_path = await message.reply_to_message.download()
    
    with open(file_path, 'r') as f:
        urls = json.load(f)
    os.remove(file_path)
    
    conn = init_db()
    c = conn.cursor()
    
    await status.edit(f"⚙️ Building DB for {len(urls)} URLs. This will run in the background.")
    
    loop = asyncio.get_running_loop()
    success_count = 0
    
    for i, url in enumerate(urls):
        c.execute("SELECT 1 FROM novels WHERE url=?", (url,))
        if c.fetchone(): continue # Skip existing
            
        if i % 10 == 0:
            await status.edit(f"⚙️ DB Build Progress: {i}/{len(urls)}")
            
        result = await loop.run_in_executor(executor, scrape_toc_worker, url)
        
        if not result.get("error"):
            c.execute("INSERT INTO novels (url, title) VALUES (?, ?)", (url, result["title"]))
            c.executemany("INSERT INTO chapters (id, novel_url, chapter_index) VALUES (?, ?, ?)", result["chapters"])
            conn.commit()
            success_count += 1
            
    conn.close()
    await status.edit(f"✅ DB Build Complete! Scraped {success_count} new TOCs.\nSend `/heal` to begin processing.")

@app.on_message(filters.command("heal"))
async def heal_cmd(client, message: Message):
    user_states[message.chat.id] = {"step": 1}
    await message.reply("📡 Enter the **Source Channel ID** (where the 80k files are):")

@app.on_message(filters.text & filters.private & ~filters.command(["start", "builddb", "heal"]))
async def state_machine(client, message: Message):
    chat_id = message.chat.id
    state = user_states.get(chat_id)
    if not state: return

    try:
        val = int(message.text.strip())
        
        if state["step"] == 1:
            state["source_chat"] = val
            state["step"] = 2
            await message.reply("✅ Got Source. Now enter the **Target OK Channel ID** (for perfect files):")
            
        elif state["step"] == 2:
            state["ok_chat"] = val
            state["step"] = 3
            await message.reply("✅ Got OK Channel. Now enter the **Target FIXED Channel ID** (for reordered/redownloaded files):")
            
        elif state["step"] == 3:
            state["fixed_chat"] = val
            state["step"] = 4
            await message.reply("✅ Got Fixed Channel. Enter the **Start Message ID**:")
            
        elif state["step"] == 4:
            state["start_msg"] = val
            state["step"] = 5
            await message.reply("✅ Got Start ID. Enter the **End Message ID**:")
            
        elif state["step"] == 5:
            state["end_msg"] = val
            state["step"] = "RUNNING"
            await message.reply("🚀 Configuration complete! Starting the Healer Engine...")
            
            # Start the background task
            asyncio.create_task(run_healing_loop(client, chat_id, state))
            
    except ValueError:
        await message.reply("⚠️ Please enter a valid number/ID.")

async def run_healing_loop(client, chat_id, config):
    status_msg = await client.send_message(chat_id, "Initializing processing loop...")
    loop = asyncio.get_running_loop()
    
    source = config["source_chat"]
    ok_chat = config["ok_chat"]
    fixed_chat = config["fixed_chat"]
    
    # Process in chunks of 200 (Telegram Limit)
    for chunk_start in range(config["start_msg"], config["end_msg"] + 1, 200):
        chunk_end = min(chunk_start + 199, config["end_msg"])
        msg_ids = list(range(chunk_start, chunk_end + 1))
        
        await status_msg.edit(f"🔄 Fetching chunk {chunk_start} to {chunk_end}...")
        messages = await client.get_messages(source, msg_ids)
        
        for msg in messages:
            if msg.empty or not msg.document or not msg.document.file_name.endswith('.epub'):
                continue
                
            epub_path = os.path.join(TEMP_DIR, f"{msg.id}.epub")
            await msg.download(file_name=epub_path)
            
            try:
                # 1. Analyze and Fix
                status, result = await loop.run_in_executor(executor, analyze_and_fix_epub, epub_path)
                
                if status == "OK":
                    # Zero-bandwidth forward via file_id
                    await client.send_document(ok_chat, document=msg.document.file_id, caption="Status: OK")
                    
                elif status == "FIXED":
                    await client.send_document(fixed_chat, document=result, caption="Status: Fixed Jumbled Spine")
                    os.remove(result)
                    
                elif status == "MISSING":
                    source_url = result
                    await client.send_message(chat_id, f"⚠️ Msg {msg.id}: Missing chapters detected. Redownloading {source_url}...")
                    
                    redownload_dir = os.path.join(TEMP_DIR, f"redownload_{msg.id}")
                    os.makedirs(redownload_dir, exist_ok=True)
                    
                    new_epub = await loop.run_in_executor(executor, redownload_worker, source_url, redownload_dir)
                    if new_epub:
                        await client.send_document(fixed_chat, document=new_epub, caption="Status: Redownloaded Missing Chapters")
                    else:
                        await client.send_message(chat_id, f"❌ Failed to redownload Msg {msg.id}.")
                        
                    shutil.rmtree(redownload_dir, ignore_errors=True)
                    
                elif status == "ERROR":
                    await client.send_message(chat_id, f"❌ Msg {msg.id} Error: {result}")
                    
            except Exception as e:
                logger.error(f"Error processing {msg.id}: {e}")
            finally:
                if os.path.exists(epub_path):
                    os.remove(epub_path)

if __name__ == "__main__":
    multiprocessing.freeze_support()
    print("🚀 Healer Bot Starting...")
    app.run()