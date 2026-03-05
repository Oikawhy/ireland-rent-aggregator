"""
AGPARS Telegram Delivery

Sends notifications to Telegram via Bot API with idempotency.

Covers T061.
"""

import time

import httpx

from packages.core.config import get_settings
from packages.observability.logger import get_logger
from packages.observability.metrics import (
    TELEGRAM_SEND_DURATION_SECONDS,
    TELEGRAM_RATE_LIMITED_TOTAL,
)
from packages.storage.delivery_log import record_delivery, was_delivered

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM SENDER
# ═══════════════════════════════════════════════════════════════════════════════


class TelegramSender:
    """Sends messages to Telegram Bot API."""

    API_BASE = "https://api.telegram.org/bot{token}"

    def __init__(self, token: str | None = None):
        settings = get_settings()
        self.token = token or settings.telegram.bot_token
        self.base_url = self.API_BASE.format(token=self.token)
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30)
        return self._client

    async def send_message(
        self,
        chat_id: int,
        text: str,
        parse_mode: str = "Markdown",
        disable_web_page_preview: bool = False,
    ) -> dict | None:
        """
        Send a text message to a Telegram chat.

        Args:
            chat_id: Telegram chat ID
            text: Message text (Markdown formatted)
            parse_mode: "Markdown" or "HTML"
            disable_web_page_preview: Disable link previews

        Returns:
            Telegram API response dict or None on failure
        """
        client = await self._get_client()

        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": disable_web_page_preview,
        }

        try:
            t0 = time.monotonic()
            resp = await client.post(
                f"{self.base_url}/sendMessage",
                json=payload,
            )
            elapsed = time.monotonic() - t0
            TELEGRAM_SEND_DURATION_SECONDS.observe(elapsed)

            if resp.status_code == 200:
                data = resp.json()
                if data.get("ok"):
                    msg_id = data["result"]["message_id"]
                    logger.debug("Message sent", chat_id=chat_id, message_id=msg_id)
                    return data["result"]
                else:
                    logger.warning(
                        "Telegram API error",
                        chat_id=chat_id,
                        description=data.get("description"),
                    )
                    return None

            elif resp.status_code == 429:
                # Rate limited by Telegram
                TELEGRAM_RATE_LIMITED_TOTAL.inc()
                retry_after = resp.json().get("parameters", {}).get("retry_after", 1)
                logger.warning(
                    "Telegram rate limit hit",
                    chat_id=chat_id,
                    retry_after=retry_after,
                )
                return None

            else:
                logger.error(
                    "Telegram HTTP error",
                    chat_id=chat_id,
                    status=resp.status_code,
                    body=resp.text[:200],
                )
                return None

        except httpx.TimeoutException:
            logger.error("Telegram request timed out", chat_id=chat_id)
            return None
        except Exception as e:
            logger.error("Telegram send failed", chat_id=chat_id, error=str(e))
            return None

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None


# ═══════════════════════════════════════════════════════════════════════════════
# DELIVERY WITH IDEMPOTENCY
# ═══════════════════════════════════════════════════════════════════════════════

# Module-level sender (lazily created)
_sender: TelegramSender | None = None


def _get_sender() -> TelegramSender:
    global _sender
    if _sender is None:
        _sender = TelegramSender()
    return _sender


async def send_notification(
    workspace_id: int,
    chat_id: int,
    event_id: int,
    message_text: str,
) -> bool:
    """
    Send a notification with idempotency check.

    Args:
        workspace_id: Workspace ID
        chat_id: Telegram chat ID
        event_id: Event outbox ID (for idempotency)
        message_text: Rendered message text

    Returns:
        True if sent (or already delivered), False on failure
    """
    # Idempotency: check if already delivered
    if was_delivered(workspace_id, event_id):
        logger.debug(
            "Event already delivered, skipping",
            workspace_id=workspace_id,
            event_id=event_id,
        )
        return True

    sender = _get_sender()
    result = await sender.send_message(chat_id=chat_id, text=message_text)

    if result:
        telegram_message_id = result.get("message_id")
        record_delivery(
            workspace_id=workspace_id,
            event_id=event_id,
            telegram_message_id=telegram_message_id,
        )
        logger.info(
            "Notification delivered",
            workspace_id=workspace_id,
            event_id=event_id,
            telegram_message_id=telegram_message_id,
        )
        return True

    return False
