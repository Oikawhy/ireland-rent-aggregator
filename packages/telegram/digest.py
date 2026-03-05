"""
AGPARS Digest Batching

Creates digest message from aggregated outbox events.
Supports pagination for large batches.

Covers T070.
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from sqlalchemy import text

from packages.observability.logger import get_logger
from packages.storage.db import get_readonly_session
from packages.telegram.templates import escape_markdown

logger = get_logger(__name__)

DIGEST_PAGE_SIZE = 10


def get_pending_events_for_workspace(workspace_id: int) -> list[dict]:
    """
    Fetch pending outbox events for a workspace (for digest batching).

    Returns:
        List of event dicts
    """
    query = """
        SELECT id, event_type, listing_raw_id, payload, created_at
        FROM ops.event_outbox
        WHERE workspace_id = :workspace_id
        AND status = 'pending'
        ORDER BY created_at ASC
    """

    with get_readonly_session() as session:
        result = session.execute(text(query), {"workspace_id": workspace_id})
        return [dict(row._mapping) for row in result.fetchall()]


def create_digest_batch(workspace_id: int) -> dict | None:
    """
    Create a digest batch from pending events.

    Aggregates ALL pending events (no truncation).

    Args:
        workspace_id: Workspace to create digest for

    Returns:
        Batch dict with message text and stats, or None if no events
    """
    events = get_pending_events_for_workspace(workspace_id)

    if not events:
        return None

    new_events = [e for e in events if e["event_type"] == "new"]
    updated_events = [e for e in events if e["event_type"] == "updated"]

    total = len(events)
    total_pages = max(1, (total + DIGEST_PAGE_SIZE - 1) // DIGEST_PAGE_SIZE)

    # Build first page message
    message = format_digest_page(events, page=1)

    return {
        "workspace_id": workspace_id,
        "total_events": total,
        "new_count": len(new_events),
        "updated_count": len(updated_events),
        "event_ids": [e["id"] for e in events],
        "first_event_id": events[0]["id"] if events else 0,
        "message": message,
        "total_pages": total_pages,
    }


def format_digest_page(events: list[dict], page: int = 1) -> str:
    """
    Format a single page of digest events.

    Args:
        events: All events
        page: Page number (1-indexed)

    Returns:
        Formatted message string
    """
    total = len(events)
    total_pages = max(1, (total + DIGEST_PAGE_SIZE - 1) // DIGEST_PAGE_SIZE)
    page = min(page, total_pages)

    start = (page - 1) * DIGEST_PAGE_SIZE
    page_events = events[start:start + DIGEST_PAGE_SIZE]

    new_events = [e for e in events if e["event_type"] == "new"]
    updated_events = [e for e in events if e["event_type"] == "updated"]

    lines = [
        f"📬 *Rental Digest* — {total} listing{'s' if total > 1 else ''}"
    ]

    if total_pages > 1:
        lines[0] += f" (page {page}/{total_pages})"

    lines.append("")

    # Determine which events are on this page
    page_new = [e for e in page_events if e["event_type"] == "new"]
    page_updated = [e for e in page_events if e["event_type"] == "updated"]

    if page_new:
        lines.append(f"🆕 *New Listings*\n")
        for i, event in enumerate(page_new, start=start + 1):
            payload = event.get("payload", {})
            lines.append(_format_digest_item(i, payload))

    if page_updated:
        lines.append(f"\n🔄 *Updated Listings*\n")
        for i, event in enumerate(page_updated, start=start + 1):
            payload = event.get("payload", {})
            lines.append(_format_digest_item(i, payload))

    return "\n".join(lines)


def build_digest_keyboard(total_pages: int, current_page: int) -> InlineKeyboardMarkup | None:
    """Build pagination keyboard for digest."""
    if total_pages <= 1:
        return None

    nav_row = []
    if current_page > 1:
        nav_row.append(InlineKeyboardButton("◀️", callback_data=f"digest_page:{current_page - 1}"))
    nav_row.append(InlineKeyboardButton(f"{current_page}/{total_pages}", callback_data="noop"))
    if current_page < total_pages:
        nav_row.append(InlineKeyboardButton("▶️", callback_data=f"digest_page:{current_page + 1}"))

    return InlineKeyboardMarkup([nav_row])


def _format_digest_item(index: int, payload: dict) -> str:
    """Format a single listing for digest display."""
    price = payload.get("price")
    try:
        price_str = f"€{int(float(price)):,}" if price else "N/A"
    except (ValueError, TypeError):
        price_str = f"€{price}" if price else "N/A"

    beds = payload.get("beds", "?")
    city = payload.get("city", "Unknown")
    county = payload.get("county", "")
    url = payload.get("url", "")
    source = payload.get("source", "")

    location = f"{city}, {county}" if county else city

    return f"{index}. {price_str} | {beds}BR | {location} | [{source}]({url})"
