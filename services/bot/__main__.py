"""
AGPARS Telegram Bot — Entry Point

Wires all handlers and starts the bot in polling mode.
Run with: python -m services.bot
"""

import sys

from telegram.ext import Application, CallbackQueryHandler, CommandHandler

from packages.core.config import get_settings
from packages.observability.logger import get_logger

logger = get_logger(__name__)


def build_application() -> Application:
    """Build the Telegram Application with all handlers registered."""
    settings = get_settings()
    token = settings.telegram.bot_token

    if not token:
        logger.error("TELEGRAM_BOT_TOKEN is not set")
        sys.exit(1)

    app = Application.builder().token(token).build()

    # ── Onboarding (T040) ────────────────────────────────────────────────
    from services.bot.handlers.onboard import handle_help, handle_start

    app.add_handler(CommandHandler("start", handle_start))
    app.add_handler(CommandHandler("help", handle_help))

    # ── Admin commands (T041) ────────────────────────────────────────────
    from services.bot.handlers.commands import (
        handle_admin,
        handle_pause,
        handle_resume,
    )

    app.add_handler(CommandHandler("admin", handle_admin))
    app.add_handler(CommandHandler("pause", handle_pause))
    app.add_handler(CommandHandler("resume", handle_resume))

    # ── Settings & Filters (T042) ────────────────────────────────────────
    from services.bot.handlers.settings import handle_filter, handle_settings

    app.add_handler(CommandHandler("settings", handle_settings))
    app.add_handler(CommandHandler("filter", handle_filter))

    # ── Show latest (T065) ───────────────────────────────────────────────
    from services.bot.handlers.show_latest import handle_latest

    app.add_handler(CommandHandler("latest", handle_latest))

    # ── Browse with pagination (T065.10) ─────────────────────────────────
    from services.bot.handlers.browse import handle_browse

    app.add_handler(CommandHandler("browse", handle_browse))

    # ── Stats (T065.11) ──────────────────────────────────────────────────
    from services.bot.handlers.stats import handle_stats

    app.add_handler(CommandHandler("stats", handle_stats))

    # ── Digest commands (T072) ───────────────────────────────────────────
    from services.bot.handlers.digest import handle_digest

    app.add_handler(CommandHandler("digest", handle_digest))

    # ── Inline Keyboard Callbacks (UI Overhaul) ──────────────────────────
    from services.bot.handlers.callbacks import handle_callback

    app.add_handler(CallbackQueryHandler(handle_callback))

    logger.info("Bot application built", handlers=len(app.handlers[0]))
    return app


def main() -> None:
    """Start the bot."""
    import os
    from packages.observability.metrics import start_metrics_server

    metrics_port = int(os.environ.get("METRICS_PORT", 8000))
    start_metrics_server(port=metrics_port)
    logger.info("Metrics server started", port=metrics_port)

    logger.info("Starting AGPARS Telegram Bot...")
    app = build_application()
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
