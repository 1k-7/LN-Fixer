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

TOKEN = os.getenv("TELEGRAM_TOKEN")
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")

DATA_DIR = "data"
TEMP_DIR = "temp_epubs"
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")

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
        
        self.download_semaphore = asyncio.Semaphore(10) 
        self.parallel_processes = 16

    async def post_init(self, application: Application):
        self.executor = ProcessPoolExecutor(max_workers=self.parallel_processes)
        logger.info(f"⚙️ ProcessPoolExecutor heavily scaled with {self.parallel_processes} parallel workers.")
        
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
                    in_memory=True,
                    max_concurrent_transmissions=10 # 🚀 UNLOCKS MTPROTO CONCURRENCY 🚀
                )
                await self.userbot.start()
                logger.info("✅ Pyrogram Userbot Connected & Concurrent Transmissions Unlocked!")
            except Exception as e:
                logger.error(f"❌ Userbot Failed: {e}")

    async def post_stop(self, application: Application):
        if self.userbot:
            await self.userbot.stop()
        if self.executor:
            self.executor.shutdown(wait=False)

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "🛠 **LN EPUB Unlocked Pipeline (With Active Retry & Strict 404 Fallback)**\n\n"
            "1. Send `/setup <supergroup_id>` to initialize Topics.\n"
            "2. Send `/process <message_link> [optional_start_id]` to start parsing."
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
            return await update.message.reply_text("⚠️ Provide the final message link: `/process https://t.me/c/123456789/5000 [optional_start_id]`")
            
        link = context.args[0]
        start_msg_id = 1
        
        # 🚀 RESUME FEATURE LOGIC 🚀
        if len(context.args) > 1:
            try:
                start_msg_id = int(context.args[1])
            except ValueError:
                return await update.message.reply_text("⚠️ Invalid start ID provided. Format: `/process <link> 2500`")

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
            f"Target ID: `{start_msg_id}` to `{end_msg_id}`\n"
            f"VPS Parallelism: `{self.parallel_processes}` Processes"
        )
        asyncio.create_task(self.run_streaming_loop(update.effective_chat.id, source_chat, end_msg_id, start_msg_id, context.bot))

    async def status_updater(self, status_msg, stats, start_msg_id, end_msg_id, process_q, upload_q):
        while True:
            try:
                await status_msg.edit_text(
                    f"🚀 **Pipeline Active (Decoupled & Accelerated)**\n"
                    f"Found {stats.total_found} valid EPUBs from ID {start_msg_id} to {end_msg_id}.\n\n"
                    f"✅ OK: {stats.success} | 🛠 Fixed: {stats.fixed}\n"
                    f"📥 Redownloaded: {stats.redownloaded} | ❌ Errors: {stats.errors}\n\n"
                    f"⚙️ Processing Queue: `{process_q.qsize()}`\n"
                    f"📤 Upload Queue: `{upload_q.qsize()}`"
                )
            except Exception:
                pass
            await asyncio.sleep(5)

    async def process_single_epub(self, msg, loop):
        log_data = []
        original_filename = msg.document.file_name
        epub_path = os.path.join(ABS_TEMP_DIR, f"{msg.id}.epub")
        
        MAX_RETRIES = 3
        download_success = False
        
        for attempt in range(MAX_RETRIES):
            async with self.download_semaphore:
                try:
                    actual_path = await msg.download(file_name=epub_path)
                    if actual_path: epub_path = actual_path
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
        
        # 🚀 ACTIVE RETRY LOOP FOR SCRAPING (Handles 502s) 🚀
        MAX_SCRAPE_RETRIES = 3
        canonical_toc, has_duplicates, scrape_err = None, False, None
        
        for scrape_attempt in range(MAX_SCRAPE_RETRIES):
            canonical_toc, has_duplicates, scrape_err = await loop.run_in_executor(self.executor, fetch_live_toc, url)
            if scrape_err and "404" not in scrape_err:
                log_data.append(f"⚠️ Scrape attempt {scrape_attempt + 1} failed: {scrape_err}. Retrying in 3s...")
                await asyncio.sleep(3)
                continue
            break # Breaks immediately if Success OR if 404 (No point retrying a dead link)
            
        fallback_needed = False
        status, result = None, None

        if scrape_err:
            if "404" in scrape_err:
                log_data.append(f"⚠️ Source URL returned 404 Not Found. Bypassing live sync.")
                fallback_needed = True
            else:
                log_data.append(f"❌ Scrape failed completely after {MAX_SCRAPE_RETRIES} attempts. Error: {scrape_err}")
                if os.path.exists(epub_path): os.remove(epub_path)
                shutil.rmtree(extract_dir, ignore_errors=True)
                return "ERROR", "Scrape Failed (Server Error)", log_data
                
        elif has_duplicates:
            log_data.append("⚠️ Canonical TOC has duplicate chapter titles. Safe sorting is impossible.")
            status = "REDOWNLOAD"
        else:
            status, result = await loop.run_in_executor(self.executor, fix_epub_spine, epub_path, extract_dir, canonical_toc, log_data)
        
        if status in ("OK", "FIXED"):
            if os.path.exists(epub_path) and result != epub_path: os.remove(epub_path)
            return status, result, log_data

        # 🚀 ACTIVE RETRY LOOP FOR REDOWNLOADING (Handles 502s) 🚀
        if status == "REDOWNLOAD":
            log_data.append("📥 Attempting fresh redownload natively (using patched sequential scraper)...")
            redownload_dir = os.path.join(ABS_TEMP_DIR, f"redownload_{msg.id}")
            os.makedirs(redownload_dir, exist_ok=True)
            
            new_epub = None
            for rd_attempt in range(3):
                new_epub = await loop.run_in_executor(self.executor, redownload_worker, url, redownload_dir)
                if new_epub: break
                log_data.append(f"⚠️ Redownload attempt {rd_attempt + 1} failed (Likely 502/Timeout). Retrying in 5s...")
                await asyncio.sleep(5)
                
            if new_epub:
                if os.path.exists(epub_path): os.remove(epub_path)
                shutil.rmtree(extract_dir, ignore_errors=True)
                log_data.append("✅ Redownload successful in absolute 1:1 order.")
                return "REDOWNLOADED", new_epub, log_data
            else:
                log_data.append("❌ Redownload completely failed after 3 attempts due to server instability.")
                if os.path.exists(epub_path): os.remove(epub_path)
                shutil.rmtree(extract_dir, ignore_errors=True)
                return "ERROR", "Redownload Failed (Server Error)", log_data

        # 🚀 STRICT 404 MATH FALLBACK 🚀
        if fallback_needed:
            log_data.append("🧮 Triggering internal Math Fallback Sorter as last resort.")
            if not os.path.exists(extract_dir):
                _, extract_dir, _ = await loop.run_in_executor(self.executor, extract_url_from_epub, epub_path)
            
            status, result = await loop.run_in_executor(self.executor, fix_epub_spine_fallback, epub_path, extract_dir, log_data)
            if os.path.exists(epub_path) and result != epub_path: os.remove(epub_path)
            return status, result, log_data
            
        if os.path.exists(epub_path): os.remove(epub_path)
        shutil.rmtree(extract_dir, ignore_errors=True)
        return "ERROR", "Unhandled state reached", log_data

    async def process_worker(self, process_queue, upload_queue, loop):
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

    async def run_streaming_loop(self, chat_id, source_chat, end_msg_id, start_msg_id, bot):
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

        process_workers = [
            asyncio.create_task(self.process_worker(process_queue, upload_queue, loop))
            for _ in range(self.parallel_processes)
        ]

        upload_workers = [
            asyncio.create_task(self.upload_worker(upload_queue, bot, target, t_ok, t_fixed, t_re, t_logs, stats))
            for _ in range(8)
        ]

        updater_task = asyncio.create_task(self.status_updater(status_msg, stats, start_msg_id, end_msg_id, process_queue, upload_queue))

        # 🚀 USES THE START MESSAGE ID PARAMETER 🚀
        for chunk_start in range(start_msg_id, end_msg_id + 1, 100):
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

        await process_queue.join()
        await upload_queue.join()

        for w in process_workers: w.cancel()
        for w in upload_workers: w.cancel()
        updater_task.cancel()

        await status_msg.edit_text(
            f"✅ **High-Throughput Pipeline Complete!**\n\n"
            f"Processed {stats.total_found} valid EPUBs from ID {start_msg_id} to {end_msg_id}.\n"
            f"✅ OK: {stats.success} | 🛠 Fixed: {stats.fixed}\n"
            f"📥 Redownloaded: {stats.redownloaded} | ❌ Errors: {stats.errors}"
        )

    def start(self):
        print("🚀 Bot Starting (Max Concurrency, Active Retry & 404 Fallback Unlocked)...")
        app = Application.builder().token(TOKEN).post_init(self.post_init).post_stop(self.post_stop).build()

        app.add_handler(CommandHandler("start", self.cmd_start))
        app.add_handler(CommandHandler("setup", self.cmd_setup))
        app.add_handler(CommandHandler("process", self.cmd_process))

        app.run_polling()

if __name__ == "__main__":
    multiprocessing.freeze_support()
    healer = HealerBot()
    healer.start()