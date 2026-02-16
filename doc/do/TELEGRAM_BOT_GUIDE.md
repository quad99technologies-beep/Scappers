# Telegram Bot Guide

Use the Telegram bot to check pipeline status and start pipelines remotely.

## Setup
1. Create a bot token with BotFather.
2. Set these environment variables (in `.env` or your shell):
   - `TELEGRAM_BOT_TOKEN` (required)
   - `TELEGRAM_ALLOWED_CHAT_IDS` (optional, comma-separated)
   - `TELEGRAM_DEFAULT_SCRAPER` (optional, e.g. `CanadaQuebec`)
3. Start the bot:
   - `python tools/telegram_bot.py`

## Commands
- `/help` - Show help
- `/whoami` - Show your chat ID
- `/ping` - Health check
- `/list` - List available scrapers
- `/status <scraper|all>` - Show status for one or all
- `/run <scraper> [fresh]` - Start pipeline if idle
- `/resume <scraper>` - Resume pipeline if idle
- `/stop <scraper>` - Stop a running pipeline
- `/runfresh <scraper>` - Start a fresh pipeline
- `/clear <scraper>` - Clear a stale lock file

## Notes
- Status is based on the platform lock file. Pipelines started from the GUI are detected.
- Pipelines started outside the platform may not show as running unless they create a lock file.
- Bot logs are written to `logs/telegram/`.
