import os
import json
import asyncio
import logging
import shutil
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters
from pyrogram import Client as UserBotClient

from lncrawl.core.sources import load_sources 
from healer_utils import extract_url_from_epub, fetch_live_toc, fix_epub_spine, redownload_worker

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
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)

class HealerBot:
    def __init__(self):
        self.executor = None
        self.userbot = None
        self.config = {}

    async def post_init(self, application: Application):
        self.executor = ProcessPoolExecutor(max_workers=4)
        
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
            "🛠 **LN EPUB On-Demand Healer Ready**\n\n"
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
            await update.message.reply_text(f"❌ Setup Failed. Make sure the Bot (not just Userbot) is an admin. Error: {e}")

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

        await update.message.reply_text(f"🚀 Streaming Engine Started!\nSource: `{source_chat}`\nTarget ID: `1` to `{end_msg_id}`")
        asyncio.create_task(self.run_streaming_loop(update.effective_chat.id, source_chat, end_msg_id, context.bot))

    async def process_single_epub(self, msg, loop):
        log_data = []
        original_filename = msg.document.file_name
        epub_path = os.path.join(TEMP_DIR, f"{msg.id}.epub")
        await msg.download(file_name=epub_path)

        url, extract_dir, err = await loop.run_in_executor(self.executor, extract_url_from_epub, epub_path)
        if err:
            if os.path.exists(epub_path): os.remove(epub_path)
            log_data.append(f"❌ Extraction Error: {err}")
            return "ERROR", err, log_data

        log_data.append(f"🔗 Source URL: `{url}`")
        canonical_toc, has_duplicates, scrape_err = await loop.run_in_executor(self.executor, fetch_live_toc, url)
        
        if scrape_err or not canonical_toc:
            shutil.rmtree(extract_dir, ignore_errors=True)
            if os.path.exists(epub_path): os.remove(epub_path)
            log_data.append(f"❌ Scraping Error: {scrape_err}")
            return "ERROR", f"Failed to scrape live TOC: {scrape_err}", log_data

        if has_duplicates:
            log_data.append("⚠️ Canonical TOC has duplicate chapter titles. Safe sorting is impossible.")
            status = "REDOWNLOAD"
            result = None
        else:
            status, result = await loop.run_in_executor(self.executor, fix_epub_spine, epub_path, extract_dir, canonical_toc, log_data)
        
        if status == "OK":
            return "OK", epub_path, log_data

        if os.path.exists(epub_path): os.remove(epub_path)
        
        if status in ("REDOWNLOAD", "MISSING"):
            log_data.append("📥 Attempting fresh redownload via lncrawl...")
            redownload_dir = os.path.join(TEMP_DIR, f"redownload_{msg.id}")
            os.makedirs(redownload_dir, exist_ok=True)
            new_epub = await loop.run_in_executor(self.executor, redownload_worker, url, redownload_dir)
            
            if new_epub:
                log_data.append("✅ Redownload successful.")
                return "REDOWNLOADED", new_epub, log_data
            else:
                log_data.append("❌ Redownload failed.")
                return "ERROR", f"Failed to redownload {url}", log_data
            
        return status, result, log_data

    async def run_streaming_loop(self, chat_id, source_chat, end_msg_id, bot):
        status_msg = await bot.send_message(chat_id=chat_id, text="Initializing processing loop...")
        loop = asyncio.get_running_loop()

        await loop.run_in_executor(None, load_sources)

        target = self.config["target_chat"]
        t_ok = self.config["topic_ok"]
        t_fixed = self.config["topic_fixed"]
        t_re = self.config["topic_redownloaded"]
        t_logs = self.config["topic_logs"]

        success, fixed, redownloaded, errors = 0, 0, 0, 0

        for chunk_start in range(1, end_msg_id + 1, 10):
            chunk_end = min(chunk_start + 9, end_msg_id)
            msg_ids = list(range(chunk_start, chunk_end + 1))

            await status_msg.edit_text(
                f"🔄 Processing IDs {chunk_start} to {chunk_end}...\n"
                f"✅ OK: {success} | 🛠 Fixed: {fixed} | 📥 Redownloaded: {redownloaded} | ❌ Errors: {errors}"
            )

            try:
                messages = await self.userbot.get_messages(source_chat, msg_ids)
            except Exception as e:
                logger.error(f"❌ Userbot failed to fetch messages for ids {chunk_start}-{chunk_end}. Error: {e}")
                await asyncio.sleep(5) 
                continue # Do not kill stream, attempt to recover on next chunk

            valid_msgs = [m for m in messages if m and m.document and m.document.file_name and m.document.file_name.endswith('.epub')]
            
            if not valid_msgs:
                continue

            tasks = [self.process_single_epub(msg, loop) for msg in valid_msgs]
            results = await asyncio.gather(*tasks)

            for msg, (status, result, log_data) in zip(valid_msgs, results):
                original_filename = msg.document.file_name
                
                log_text = f"📄 **File:** `{original_filename}`\n⚙️ **Status:** `{status}`\n" + "\n".join(log_data)
                try:
                    await bot.send_message(chat_id=target, text=log_text, message_thread_id=t_logs)
                except Exception as e:
                    logger.error(f"Failed to send log for {msg.id}: {e}")

                # Safely send directly using the underlying f-handle faking original_filename 
                # This explicitly avoids temp directory file overwrite collisions between threads
                try:
                    if status == "OK":
                        with open(result, 'rb') as f:
                            await bot.send_document(chat_id=target, document=f, filename=original_filename, message_thread_id=t_ok)
                        os.remove(result)
                        success += 1
                        
                    elif status == "FIXED":
                        with open(result, 'rb') as f:
                            await bot.send_document(chat_id=target, document=f, filename=original_filename, message_thread_id=t_fixed)
                        os.remove(result)
                        fixed += 1
                        
                    elif status == "REDOWNLOADED":
                        with open(result, 'rb') as f:
                            await bot.send_document(chat_id=target, document=f, filename=original_filename, message_thread_id=t_re)
                        shutil.rmtree(os.path.dirname(result), ignore_errors=True)
                        redownloaded += 1
                        
                    elif status == "ERROR":
                        errors += 1
                except Exception as e:
                    logger.error(f"Routing failed for {msg.id}: {e}")

        await status_msg.edit_text(f"✅ Streaming Complete! Processed up to ID {end_msg_id}.")

    def start(self):
        print("🚀 Bot Starting (Strict Title Matching & Chunk Sorting)...")
        app = Application.builder().token(TOKEN).post_init(self.post_init).post_stop(self.post_stop).build()

        app.add_handler(CommandHandler("start", self.cmd_start))
        app.add_handler(CommandHandler("setup", self.cmd_setup))
        app.add_handler(CommandHandler("process", self.cmd_process))

        app.run_polling()

if __name__ == "__main__":
    multiprocessing.freeze_support()
    healer = HealerBot()
    healer.start()