"""
AGPARS Telegram Card Rendering

Renders listing data into Telegram-formatted notification cards.
Supports NEW and UPDATED event types, with lease-unknown label (T063).

Covers T062 + T063 + T078.
"""

from packages.observability.logger import get_logger
from packages.telegram.templates import (
    DEFAULT_LISTING_TEMPLATE,
    escape_markdown,
    render_listing_message,
    render_update_message,
    truncate_text,
)

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# DUPLICATE LABELS  (T078)
# ═══════════════════════════════════════════════════════════════════════════════


def _get_duplicate_label(listing_data: dict) -> str:
    """
    Generate duplicate source label.

    If linked_sources is provided, shows "🔗 Also on: source1, source2"

    Returns:
        Label string or empty string
    """
    linked = listing_data.get("linked_sources", [])
    if not linked:
        return ""

    sources_str = ", ".join(s.title() for s in linked)
    return f"🔗 *Also on*: {sources_str}"


def render_card(
    listing_data: dict,
    event_type: str,
    workspace_template: str | None = None,
) -> str:
    """
    Render a notification card for Telegram delivery.

    Args:
        listing_data: Listing payload dict
        event_type: "new" or "updated"
        workspace_template: Custom workspace template (optional)

    Returns:
        Telegram Markdown-formatted message string
    """
    if event_type == "updated":
        return _render_update_card(listing_data)
    return _render_listing_card(listing_data, workspace_template)


def _render_listing_card(listing_data: dict, template: str | None = None) -> str:
    """Render a NEW listing notification card."""

    # Build base card via templates module
    message = render_listing_message(listing_data, template)

    # Append lease-unknown label (T063)
    lease_label = _get_lease_label(listing_data)
    if lease_label:
        message += f"\n{lease_label}"

    # Append photo link if available
    photo_url = listing_data.get("first_photo_url")
    if photo_url:
        message += f"\n📸 [Photo]({photo_url})"

    # Append duplicate label (T078)
    dup_label = _get_duplicate_label(listing_data)
    if dup_label:
        message += f"\n{dup_label}"

    return message


def _render_update_card(listing_data: dict) -> str:
    """Render an UPDATED listing notification card."""
    changes = listing_data.get("changes", {})

    if not changes:
        # Fallback: render as new listing
        return _render_listing_card(listing_data)

    message = render_update_message(listing_data, changes)

    # Append lease-unknown label (T063)
    lease_label = _get_lease_label(listing_data)
    if lease_label:
        message += f"\n{lease_label}"

    # Append duplicate label (T078)
    dup_label = _get_duplicate_label(listing_data)
    if dup_label:
        message += f"\n{dup_label}"

    return message


# ═══════════════════════════════════════════════════════════════════════════════
# LEASE LABELS  (T063)
# ═══════════════════════════════════════════════════════════════════════════════


def _get_lease_label(listing_data: dict) -> str:
    """
    Generate lease length label.

    If lease_length_unknown is True, shows "📋 Lease: Unknown"
    If lease_length_months is set, shows the duration
    Otherwise, returns empty string
    """
    if listing_data.get("lease_length_unknown"):
        return "📋 *Lease*: Unknown"

    months = listing_data.get("lease_length_months")
    if months:
        if months >= 12:
            years = months // 12
            remainder = months % 12
            if remainder:
                return f"📋 *Lease*: {years}y {remainder}m"
            return f"📋 *Lease*: {years} year{'s' if years > 1 else ''}"
        return f"📋 *Lease*: {months} month{'s' if months > 1 else ''}"

    return ""
