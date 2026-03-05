"""
AGPARS Message Manager

Tracks and deletes previous menu messages to keep chat clean.
Notification messages (instant/digest) are NEVER deleted.

Error recovery:
 - Flood control (RetryAfter): waits and retries once
 - Message not found: sends a new message as fallback
"""

import asyncio

from telegram.error import BadRequest, RetryAfter

from packages.observability.logger import get_logger

logger = get_logger(__name__)


async def send_menu_message(
    context,
    chat_id: int,
    text: str,
    reply_markup=None,
    parse_mode: str = "Markdown",
    disable_web_page_preview: bool = True,
) -> int:
    """
    Send a new menu message and delete the previous one.

    Returns:
        The new message ID
    """
    # Delete previous menu message
    prev_msg_id = context.chat_data.get("last_menu_msg_id")
    if prev_msg_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=prev_msg_id)
        except Exception:
            pass  # Message may already be deleted or too old

    # Send new message (with flood control retry)
    try:
        msg = await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
        )
    except RetryAfter as e:
        wait = min(e.retry_after + 1, 60)
        logger.warning("Flood control on send, waiting", seconds=wait)
        await asyncio.sleep(wait)
        msg = await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
        )

    # Track new message ID
    context.chat_data["last_menu_msg_id"] = msg.message_id
    return msg.message_id


async def edit_menu_message(
    context,
    chat_id: int,
    message_id: int,
    text: str,
    reply_markup=None,
    parse_mode: str = "Markdown",
) -> bool:
    """
    Edit an existing menu message in-place.

    Error recovery:
     - RetryAfter (flood): waits and retries edit
     - BadRequest "not found" / "not modified": falls back to new message
    """
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            disable_web_page_preview=True,
        )
        return True
    except RetryAfter as e:
        wait = min(e.retry_after + 1, 60)
        logger.warning("Flood control on edit, waiting", seconds=wait)
        await asyncio.sleep(wait)
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
            )
            return True
        except Exception as e2:
            logger.warning("Edit retry failed, sending new message", error=str(e2))
            await send_menu_message(
                context, chat_id, text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
            )
            return False
    except BadRequest as e:
        err = str(e).lower()
        if "not modified" in err:
            # Content unchanged, not an error
            return True
        logger.warning("Edit failed, sending new message", error=str(e))
        await send_menu_message(
            context, chat_id, text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
        )
        return False
    except Exception as e:
        logger.warning("Edit failed, sending new message", error=str(e))
        await send_menu_message(
            context, chat_id, text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
        )
        return False


async def delete_menu_message(context, chat_id: int) -> None:
    """Delete the tracked menu message."""
    prev_msg_id = context.chat_data.get("last_menu_msg_id")
    if prev_msg_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=prev_msg_id)
        except Exception:
            pass
        context.chat_data.pop("last_menu_msg_id", None)
