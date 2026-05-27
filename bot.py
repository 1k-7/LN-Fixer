import os
import json
import asyncio
import logging
import shutil
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from pyrogram import Client as UserBotClient

from lncrawl.core.sources import load_sources 
from healer_utils import extract_url_from_epub, fetch_live_toc, fix_epub_spine, fix_epub_spine_fallback, redownload_worker

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# Config
TOKEN = os.getenv("TELEGRAM_TOKEN")
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")

DATA_DIR = "data"
TEMP_DIR = "temp_epubs"
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")

# Force absolute paths
ABS_TEMP_DIR = os.path.abspath(TEMP_DIR)
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(ABS_TEMP_DIR, exist_ok=True)

class PipelineStats:
    def __init__(self):
        self.processed = 0
        self.success = 0
        self.fixed = 0
        self.redownloaded = 0
        self.errors = 0
        self.total_found = 0

class HealerBot:
    def __init__(self):
        self.executor = None
        self.userbot = None
        self.config = {}
        
        # 🚀 VPS NETWORK TUNING (48GB RAM handles this perfectly)
        self.download_semaphore = asyncio.Semaphore(10) 
        
        # 🚀 VPS CPU TUNING (Force massive parallel scraping regardless of 4 physical cores)
        self.parallel_processes = 16

    async def post_init(self, application: Application):
        self.executor = ProcessPoolExecutor(max_workers=self.parallel_processes)
        logger.info(f"⚙️ ProcessPoolExecutor aggressively spun up with {self.parallel_processes} parallel workers.")
        
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r') as f:
                self.config = json.load(f)

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

    async def post_stop(self, application: Application):
        if self.userbot:
            await self.userbot.stop()
        if self.executor:
            self.executor.shutdown(wait=False)

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "🛠 **LN EPUB Unlocked Pipeline (With 404 Math Fallback)**\n\n"
            "1. Send `/setup <supergroup_id>` to initialize Topics.\n"
            "2. Send `/process <message_link>` to start parsing."
        )

    async def cmd_setup(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            return await update.message.reply_text("⚠️ Provide a supergroup ID: `/setup -100123456789`")
            
        try:
            target_chat = int(context.args[0])
            status_msg = await update.message.reply_text(f"⚙️ Initializing Forum Topics via standard Bot API...")
            
            topic_ok = await context.bot.create_forum_topic(chat_id=target_chat, name="✅ NO Changes")
            topic_fixed = await context.bot.create_forum_topic(chat_id=target_chat, name="🛠 FIXED")
            topic_redownload = await context.bot.create_forum_topic(chat_id=target_chat, name="📥 Redownloaded")
            topic_logs = await context.bot.create_forum_topic(chat_id=target_chat, name="📜 Logs")
            
            self.config = {
                "target_chat": target_chat,
                "topic_ok": topic_ok.message_thread_id,
                "topic_fixed": topic_fixed.message_thread_id,
                "topic_redownloaded": topic_redownload.message_thread_id,
                "topic_logs": topic_logs.message_thread_id
            }
            
            with open(CONFIG_FILE, 'w') as f:
                json.dump(self.config, f) 
                
            await status_msg.edit_text("✅ Topics created and saved successfully! Send `/process <message_link>` to begin.")
            
        except Exception as e:
            await update.message.reply_text(f"❌ Setup Failed. Make sure the Bot is an admin. Error: {e}")

    async def cmd_process(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.config:
            return await update.message.reply_text("⚠️ Run `/setup` first to create the Topics.")
            
        if not context.args:
            return await update.message.reply_text("⚠️ Provide the final message link: `/process https://t.me/c/123456789/5000`")
            
        link = context.args[0]
        try:
            parts = link.rstrip('/').split('/')
            end_msg_id = int(parts[-1])
            
            if 'c' in parts:
                source_chat = int("-100" + parts[-2])
            else:
                source_chat = parts[-2]
                
        except Exception:
            return await update.message.reply_text("⚠️ Invalid message link format.")

        await update.message.reply_text(
            f"🚀 Unlocked High-Throughput Pipeline Started!\n"
            f"Source: `{source_chat}`\n"
            f"Target ID: `1` to `{end_msg_id}`\n"
            f"VPS Parallelism: `{self.parallel_processes}` Processes"
        )
        asyncio.create_task(self.run_streaming_loop(update.effective_chat.id, source_chat, end_msg_id, context.bot))

    async def status_updater(self, status_msg, stats, end_msg_id, process_q, upload_q):
        while True:
            try:
                await status_msg.edit_text(
                    f"🚀 **Pipeline Active (Decoupled & Accelerated)**\n"
                    f"Found {stats.total_found} valid EPUBs up to ID {end_msg_id}.\n\n"
                    f"✅ OK: {stats.success} | 🛠 Fixed: {stats.fixed}\n"
                    f"📥 Redownloaded: {stats.redownloaded} | ❌ Errors: {stats.errors}\n\n"
                    f"⚙️ Processing Queue: `{process_q.qsize()}`\n"
                    f"📤 Upload Queue: `{upload_q.qsize()}`"
                )
            except Exception:
                pass
            await asyncio.sleep(5)

    async def process_single_epub(self, msg, loop):
        """Core scraping/zipping logic"""
        log_data = []
        original_filename = msg.document.file_name
        epub_path = os.path.join(ABS_TEMP_DIR, f"{msg.id}.epub")
        
        MAX_RETRIES = 3
        download_success = False
        
        for attempt in range(MAX_RETRIES):
            async with self.download_semaphore:
                try:
                    actual_path = await msg.download(file_name=epub_path)
                    if actual_path:
                        epub_path = actual_path
                except Exception as e:
                    logger.warning(f"Download attempt {attempt + 1} failed for {original_filename}: {e}")

            if os.path.exists(epub_path):
                file_size = os.path.getsize(epub_path)
                if file_size >= 1024: 
                    download_success = True
                    break
                else:
                    os.remove(epub_path)
            
            await asyncio.sleep(2 * (attempt + 1))
            
        if not download_success:
            log_data.append(f"❌ Network drop detected. Failed to download a valid file after {MAX_RETRIES} attempts.")
            return "ERROR", "Corrupt Empty File", log_data

        url, extract_dir, err = await loop.run_in_executor(self.executor, extract_url_from_epub, epub_path)
        if err:
            if os.path.exists(epub_path): os.remove(epub_path)
            log_data.append(f"❌ Extraction Error: {err}")
            return "ERROR", err, log_data

        log_data.append(f"🔗 Source URL: `{url}`")
        
        # 1. Attempt standard live TOC sync
        canonical_toc, has_duplicates, scrape_err = await loop.run_in_executor(self.executor, fetch_live_toc, url)
        
        # 2. IF 404 DEAD LINK -> Use Math Fallback
        if scrape_err or not canonical_toc:
            log_data.append(f"⚠️ Source URL is dead (404 Not Found). Triggering internal Math Fallback Sorter.")
            status, result = await loop.run_in_executor(self.executor, fix_epub_spine_fallback, epub_path, extract_dir, log_data)
            
        # 3. IF Live TOC Duplicate Collision -> Force Redownload
        elif has_duplicates:
            log_data.append("⚠️ Canonical TOC has duplicate chapter titles. Safe sorting is impossible.")
            status = "REDOWNLOAD"
            result = None
            
        # 4. Normal 1:1 Live Sync
        else:
            status, result = await loop.run_in_executor(self.executor, fix_epub_spine, epub_path, extract_dir, canonical_toc, log_data)
        
        if status == "OK":
            return "OK", epub_path, log_data

        if os.path.exists(epub_path): os.remove(epub_path)
        
        if status in ("REDOWNLOAD", "MISSING"):
            log_data.append("📥 Attempting fresh redownload natively (using patched sequential scraper)...")
            redownload_dir = os.path.join(ABS_TEMP_DIR, f"redownload_{msg.id}")
            os.makedirs(redownload_dir, exist_ok=True)
            new_epub = await loop.run_in_executor(self.executor, redownload_worker, url, redownload_dir)
            
            if new_epub:
                log_data.append("✅ Redownload successful in absolute 1:1 order.")
                return "REDOWNLOADED", new_epub, log_data
            else:
                log_data.append("❌ Redownload failed. The live URL is completely dead or 404.")
                return "ERROR", f"Failed to redownload {url}", log_data
            
        return status, result, log_data

    async def process_worker(self, process_queue, upload_queue, loop):
        """Pulls from process belt -> Fixes File -> Pushes to Upload Belt."""
        while True:
            msg = await process_queue.get()
            try:
                status, result, log_data = await self.process_single_epub(msg, loop)
                await upload_queue.put((msg, status, result, log_data))
            except Exception as e:
                logger.error(f"Worker exception on msg {msg.id}: {e}")
                await upload_queue.put((msg, "ERROR", None, [f"Fatal exception: {e}"]))
            finally:
                process_queue.task_done()

    async def upload_worker(self, upload_queue, bot, target, t_ok, t_fixed, t_re, t_logs, stats):
        """Pulls from Upload Belt -> Sends to Telegram. Blocks network, not CPU."""
        while True:
            msg, status, result, log_data = await upload_queue.get()
            try:
                original_filename = msg.document.file_name
                log_text = f"📄 **File:** `{original_filename}`\n⚙️ **Status:** `{status}`\n" + "\n".join(log_data)
                
                try:
                    await bot.send_message(chat_id=target, text=log_text, message_thread_id=t_logs)
                except Exception as e:
                    logger.error(f"Failed to send log for {msg.id}: {e}")

                try:
                    if status == "OK":
                        with open(result, 'rb') as f:
                            await bot.send_document(chat_id=target, document=f, filename=original_filename, message_thread_id=t_ok)
                        os.remove(result)
                        stats.success += 1
                        
                    elif status == "FIXED":
                        with open(result, 'rb') as f:
                            await bot.send_document(chat_id=target, document=f, filename=original_filename, message_thread_id=t_fixed)
                        os.remove(result)
                        stats.fixed += 1
                        
                    elif status == "REDOWNLOADED":
                        with open(result, 'rb') as f:
                            await bot.send_document(chat_id=target, document=f, filename=original_filename, message_thread_id=t_re)
                        shutil.rmtree(os.path.dirname(result), ignore_errors=True)
                        stats.redownloaded += 1
                        
                    elif status == "ERROR":
                        stats.errors += 1
                        
                except Exception as e:
                    logger.error(f"Routing failed for {msg.id}: {e}")
                    stats.errors += 1
            except Exception as e:
                logger.error(f"Uploader exception on msg {msg.id}: {e}")
                stats.errors += 1
            finally:
                stats.processed += 1
                upload_queue.task_done()

    async def run_streaming_loop(self, chat_id, source_chat, end_msg_id, bot):
        status_msg = await bot.send_message(chat_id=chat_id, text="Spinning up 3-Stage Pipeline...")
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, load_sources)

        target = self.config["target_chat"]
        t_ok = self.config["topic_ok"]
        t_fixed = self.config["topic_fixed"]
        t_re = self.config["topic_redownloaded"]
        t_logs = self.config["topic_logs"]

        stats = PipelineStats()
        process_queue = asyncio.Queue()
        upload_queue = asyncio.Queue()

        # SPAWN MAX WORKERS (16 concurrent processes overriding physical cores)
        process_workers = [
            asyncio.create_task(self.process_worker(process_queue, upload_queue, loop))
            for _ in range(self.parallel_processes)
        ]

        # NETWORK SCALING: 8 dedicated uploaders
        upload_workers = [
            asyncio.create_task(self.upload_worker(upload_queue, bot, target, t_ok, t_fixed, t_re, t_logs, stats))
            for _ in range(8)
        ]

        # UI Task
        updater_task = asyncio.create_task(self.status_updater(status_msg, stats, end_msg_id, process_queue, upload_queue))

        # PRODUCER LOOP
        for chunk_start in range(1, end_msg_id + 1, 100):
            chunk_end = min(chunk_start + 99, end_msg_id)
            msg_ids = list(range(chunk_start, chunk_end + 1))

            try:
                messages = await self.userbot.get_messages(source_chat, msg_ids)
                valid_msgs = [m for m in messages if m and m.document and m.document.file_name and m.document.file_name.endswith('.epub')]
                for m in valid_msgs:
                    stats.total_found += 1
                    await process_queue.put(m)
            except Exception as e:
                logger.error(f"❌ Userbot failed to fetch messages for ids {chunk_start}-{chunk_end}. Error: {e}")
                await asyncio.sleep(5) 

        # Wait for all processing to finish
        await process_queue.join()
        
        # Wait for all uploads to finish
        await upload_queue.join()

        # Kill background tasks
        for w in process_workers: w.cancel()
        for w in upload_workers: w.cancel()
        updater_task.cancel()

        await status_msg.edit_text(
            f"✅ **High-Throughput Pipeline Complete!**\n\n"
            f"Processed {stats.total_found} valid EPUBs up to ID {end_msg_id}.\n"
            f"✅ OK: {stats.success} | 🛠 Fixed: {stats.fixed}\n"
            f"📥 Redownloaded: {stats.redownloaded} | ❌ Errors: {stats.errors}"
        )

    def start(self):
        print("🚀 Bot Starting (Max Concurrency & 404 Fallback Unlocked)...")
        app = Application.builder().token(TOKEN).post_init(self.post_init).post_stop(self.post_stop).build()

        app.add_handler(CommandHandler("start", self.cmd_start))
        app.add_handler(CommandHandler("setup", self.cmd_setup))
        app.add_handler(CommandHandler("process", self.cmd_process))

        app.run_polling()

if __name__ == "__main__":
    multiprocessing.freeze_support()
    healer = HealerBot()
    healer.start()