import os
import json
import asyncio
import logging
import shutil
import multiprocessing
import concurrent.futures
import gc
import random
from concurrent.futures import ProcessPoolExecutor

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters
from telegram.error import RetryAfter
from pyrogram import Client as UserBotClient

from lncrawl.core.sources import load_sources 
from healer_utils import init_db, scrape_toc_worker, analyze_and_fix_epub, redownload_worker, DB_FILE

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# Config
TOKEN = os.getenv("TELEGRAM_TOKEN")
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")

DATA_DIR = "data"
TEMP_DIR = "temp_epubs"
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)

class HealerBot:
    def __init__(self):
        self.executor = None
        self.userbot = None
        self.user_states = {}

    async def post_init(self, application: Application):
        self.executor = ProcessPoolExecutor(max_workers=3)
        
        if SESSION_STRING and API_ID:
            try:
                self.userbot = UserBotClient(
                    "healer_userbot",
                    api_id=int(API_ID),
                    api_hash=API_HASH,
                    session_string=SESSION_STRING,
                    in_memory=True
                )
                await self.userbot.start()
                logger.info("✅ Pyrogram Userbot Connected!")
            except Exception as e:
                logger.error(f"❌ Userbot Failed: {e}")
        else:
            logger.error("❌ CRITICAL: SESSION_STRING missing. Userbot is required to scan channel history.")

    async def post_stop(self, application: Application):
        if self.userbot:
            await self.userbot.stop()
        if self.executor:
            self.executor.shutdown(wait=False)

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "🛠 **LN EPUB Healer Ready** (Pure Requests Engine)\n\n"
            "1. Send me your URLs JSON file and reply to it with `/builddb` to build the TOC database.\n"
            "2. Once built, send `/heal` to start the channel processing."
        )

    async def cmd_builddb(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.message.reply_to_message or not update.message.reply_to_message.document:
            return await update.message.reply_text("Reply to a JSON document with /builddb!")

        status_msg = await update.message.reply_text("📥 Downloading JSON...")
        
        file = await update.message.reply_to_message.document.get_file()
        temp_path = os.path.join(DATA_DIR, "temp_urls.json")
        await file.download_to_drive(temp_path)

        with open(temp_path, 'r', encoding='utf-8') as f:
            urls = json.load(f)
        os.remove(temp_path)

        conn = init_db()
        c = conn.cursor()

        await status_msg.edit_text("⚙️ Filtering out existing URLs from the database...")

        urls_to_process = []
        for url in urls:
            c.execute("SELECT 1 FROM novels WHERE url=?", (url,))
            if not c.fetchone():
                urls_to_process.append(url)

        total = len(urls_to_process)
        if total == 0:
            conn.close()
            return await status_msg.edit_text("✅ All URLs are already in the database! Send `/heal` to begin processing.")

        loop = asyncio.get_running_loop()

        await status_msg.edit_text("⚙️ Booting lncrawl core architecture...")
        await loop.run_in_executor(None, load_sources)

        # 80 Workers ensures we maintain ~240 active connections safely due to FanMTL's internal pagination threads
        MAX_WORKERS = 80 
        await status_msg.edit_text(f"🚀 Spooling up Pure Requests Engine with {MAX_WORKERS} workers...")
        
        queue = asyncio.Queue()
        for u in urls_to_process:
            queue.put_nowait((u, 0))

        success_count = 0
        processed_count = 0
        failed_permanently = 0
        active_retries = 0
        
        novel_data_batch = []
        chapter_data_batch = []
        is_running = True
        db_lock = asyncio.Lock()

        # --- THE WORKER TASK ---
        async def scraper_worker(pool):
            nonlocal success_count, processed_count, failed_permanently, active_retries
            while not queue.empty():
                url, attempts = queue.get_nowait()
                
                try:
                    res = await asyncio.wait_for(
                        loop.run_in_executor(pool, scrape_toc_worker, url), 
                        timeout=35.0 
                    )
                except asyncio.TimeoutError:
                    res = {"url": url, "error": "Timeout Error"}
                except Exception as e:
                    res = {"url": url, "error": str(e)}

                if res.get("error"):
                    if attempts < 3:
                        async with db_lock:
                            active_retries += 1
                        queue.put_nowait((url, attempts + 1))
                        await asyncio.sleep(1.0)
                    else:
                        async with db_lock:
                            processed_count += 1
                            failed_permanently += 1
                            active_retries = max(0, active_retries - 1)
                        logger.error(f"❌ DEAD: {url} - {res.get('error')}")
                else:
                    async with db_lock:
                        processed_count += 1
                        success_count += 1
                        if attempts > 0:
                            active_retries = max(0, active_retries - 1)
                        novel_data_batch.append((res["url"], res.get("title", "Unknown")))
                        chapter_data_batch.extend(res.get("chapters", []))

                queue.task_done()

        # --- THE UI/DB FLUSHER TASK ---
        async def ui_db_flusher():
            last_processed = -1
            last_retries = -1
            while is_running or novel_data_batch:
                await asyncio.sleep(4) 
                
                async with db_lock:
                    if novel_data_batch:
                        c.executemany("INSERT INTO novels (url, title) VALUES (?, ?)", novel_data_batch)
                        c.executemany("INSERT INTO chapters (id, novel_url, chapter_index) VALUES (?, ?, ?)", chapter_data_batch)
                        conn.commit()
                        novel_data_batch.clear()
                        chapter_data_batch.clear()
                        
                if processed_count > last_processed or active_retries != last_retries:
                    try:
                        await status_msg.edit_text(
                            f"⚡ Pure Requests Engine: {processed_count}/{total}\n"
                            f"✅ Success: {success_count} | ❌ Failed: {failed_permanently}\n"
                            f"🔄 Active Retries in Queue: {active_retries}\n"
                            f"Workers Active: {MAX_WORKERS} (Tuned for FanMTL)"
                        )
                    except RetryAfter as e:
                        await asyncio.sleep(e.retry_after) 
                    except Exception:
                        pass 
                    last_processed = processed_count
                    last_retries = active_retries
                    gc.collect() 

        # Launch Tasks
        flusher_task = asyncio.create_task(ui_db_flusher())
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            workers = [asyncio.create_task(scraper_worker(pool)) for _ in range(MAX_WORKERS)]
            await asyncio.gather(*workers)
            
        is_running = False
        await flusher_task 
        conn.close()
        
        await status_msg.edit_text(f"✅ DB Build Complete!\n✅ Scraped: {success_count}\n❌ Failed: {failed_permanently}\nSend `/heal` to begin processing.")

    async def cmd_heal(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        self.user_states[chat_id] = {"step": 1}
        await update.message.reply_text("📡 Enter the **Source Channel ID** (where the 80k files are):")

    async def handle_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        state = self.user_states.get(chat_id)
        if not state: return

        try:
            val = int(update.message.text.strip())

            if state["step"] == 1:
                state["source_chat"] = val
                state["step"] = 2
                await update.message.reply_text("✅ Got Source. Now enter the **Target OK Channel ID** (for perfect files):")

            elif state["step"] == 2:
                state["ok_chat"] = val
                state["step"] = 3
                await update.message.reply_text("✅ Got OK Channel. Now enter the **Target FIXED Channel ID** (for reordered/redownloaded files):")

            elif state["step"] == 3:
                state["fixed_chat"] = val
                state["step"] = 4
                await update.message.reply_text("✅ Got Fixed Channel. Enter the **Start Message ID**:")

            elif state["step"] == 4:
                state["start_msg"] = val
                state["step"] = 5
                await update.message.reply_text("✅ Got Start ID. Enter the **End Message ID**:")

            elif state["step"] == 5:
                state["end_msg"] = val
                state["step"] = "RUNNING"
                await update.message.reply_text("🚀 Configuration complete! Starting the Healer Engine...")

                asyncio.create_task(self.run_healing_loop(chat_id, state, context.bot))

        except ValueError:
            await update.message.reply_text("⚠️ Please enter a valid number/ID.")

    async def run_healing_loop(self, chat_id, config, bot):
        status_msg = await bot.send_message(chat_id=chat_id, text="Initializing processing loop...")
        loop = asyncio.get_running_loop()

        source = config["source_chat"]
        ok_chat = config["ok_chat"]
        fixed_chat = config["fixed_chat"]

        for chunk_start in range(config["start_msg"], config["end_msg"] + 1, 200):
            chunk_end = min(chunk_start + 199, config["end_msg"])
            msg_ids = list(range(chunk_start, chunk_end + 1))

            await status_msg.edit_text(f"🔄 Fetching chunk {chunk_start} to {chunk_end} via Userbot...")

            try:
                messages = await self.userbot.get_messages(source, msg_ids)
            except Exception as e:
                await bot.send_message(chat_id=chat_id, text=f"❌ Userbot failed to fetch messages. Is it an admin? Error: {e}")
                return

            for msg in messages:
                if msg.empty or not msg.document or not msg.document.file_name.endswith('.epub'):
                    continue

                epub_path = os.path.join(TEMP_DIR, f"{msg.id}.epub")
                await msg.download(file_name=epub_path)

                try:
                    status, result = await loop.run_in_executor(self.executor, analyze_and_fix_epub, epub_path)

                    if status == "OK":
                        await self.userbot.send_document(ok_chat, document=msg.document.file_id, caption="Status: OK")

                    elif status == "FIXED":
                        await self.userbot.send_document(fixed_chat, document=result, caption="Status: Fixed Jumbled Spine")
                        os.remove(result)

                    elif status == "MISSING":
                        source_url = result
                        await bot.send_message(chat_id=chat_id, text=f"⚠️ Msg {msg.id}: Missing chapters detected. Redownloading {source_url}...")

                        redownload_dir = os.path.join(TEMP_DIR, f"redownload_{msg.id}")
                        os.makedirs(redownload_dir, exist_ok=True)

                        new_epub = await loop.run_in_executor(self.executor, redownload_worker, source_url, redownload_dir)
                        if new_epub:
                            await self.userbot.send_document(fixed_chat, document=new_epub, caption="Status: Redownloaded Missing Chapters")
                        else:
                            await bot.send_message(chat_id=chat_id, text=f"❌ Failed to redownload Msg {msg.id}.")

                        shutil.rmtree(redownload_dir, ignore_errors=True)

                    elif status == "ERROR":
                        await bot.send_message(chat_id=chat_id, text=f"❌ Msg {msg.id} Error: {result}")

                except Exception as e:
                    logger.error(f"Error processing {msg.id}: {e}")
                finally:
                    if os.path.exists(epub_path):
                        os.remove(epub_path)

    def start(self):
        print("🚀 Bot Starting (Native Architecture Rep)...")
        app = Application.builder().token(TOKEN).post_init(self.post_init).post_stop(self.post_stop).build()

        app.add_handler(CommandHandler("start", self.cmd_start))
        app.add_handler(CommandHandler("builddb", self.cmd_builddb))
        app.add_handler(CommandHandler("heal", self.cmd_heal))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_text))

        app.run_polling()

if __name__ == "__main__":
    multiprocessing.freeze_support()
    healer = HealerBot()
    healer.start()
