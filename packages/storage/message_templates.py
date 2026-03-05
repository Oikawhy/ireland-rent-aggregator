"""
AGPARS Message Template Storage Module

CRUD operations for workspace message templates.
"""

from datetime import datetime

from sqlalchemy import delete, select, update

from packages.observability.logger import get_logger
from packages.storage.db import get_readonly_session, get_session
from packages.storage.models import MessageTemplate

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# DEFAULT TEMPLATE
# ═══════════════════════════════════════════════════════════════════════════════


DEFAULT_TEMPLATE = """🏠 *{property_type}* in *{city}*

💰 *{price}* /month
🛏️ {beds} beds | 🛁 {baths} baths
📍 {area_text}

{lease_info}

🔗 [View Listing]({url})
📌 Source: {source}"""


# ═══════════════════════════════════════════════════════════════════════════════
# CREATE / UPDATE
# ═══════════════════════════════════════════════════════════════════════════════


def upsert_template(workspace_id: int, template: str) -> int:
    """
    Create or update a template for a workspace.

    Args:
        workspace_id: Workspace ID
        template: Template string with placeholders

    Returns:
        Template ID
    """
    existing = _get_template_record(workspace_id)

    if existing:
        # Update existing
        with get_session() as session:
            stmt = (
                update(MessageTemplate)
                .where(MessageTemplate.workspace_id == workspace_id)
                .values(template=template, updated_at=datetime.utcnow())
            )
            session.execute(stmt)
            logger.info("Template updated", workspace_id=workspace_id)
            return existing.id
    else:
        # Create new
        with get_session() as session:
            record = MessageTemplate(
                workspace_id=workspace_id,
                template=template,
            )
            session.add(record)
            session.flush()
            template_id = record.id
            logger.info("Template created", workspace_id=workspace_id, template_id=template_id)
            return template_id


# ═══════════════════════════════════════════════════════════════════════════════
# READ
# ═══════════════════════════════════════════════════════════════════════════════


def get_template(workspace_id: int) -> str:
    """
    Get template for a workspace.

    Returns default template if none is set.
    """
    record = _get_template_record(workspace_id)
    if record:
        return record.template
    return DEFAULT_TEMPLATE


def get_template_record(workspace_id: int) -> dict | None:
    """Get full template record as dictionary."""
    record = _get_template_record(workspace_id)
    if record:
        return {
            "id": record.id,
            "workspace_id": record.workspace_id,
            "template": record.template,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
        }
    return None


def _get_template_record(workspace_id: int) -> MessageTemplate | None:
    """Get MessageTemplate ORM object."""
    with get_readonly_session() as session:
        query = select(MessageTemplate).where(MessageTemplate.workspace_id == workspace_id)
        result = session.execute(query)
        return result.scalar_one_or_none()


# ═══════════════════════════════════════════════════════════════════════════════
# DELETE
# ═══════════════════════════════════════════════════════════════════════════════


def delete_template(workspace_id: int) -> bool:
    """
    Delete template for a workspace (reverts to default).

    Returns:
        True if template was deleted
    """
    with get_session() as session:
        stmt = delete(MessageTemplate).where(MessageTemplate.workspace_id == workspace_id)
        result = session.execute(stmt)
        if result.rowcount > 0:
            logger.info("Template deleted", workspace_id=workspace_id)
            return True
        return False


def reset_to_default(workspace_id: int) -> bool:
    """Alias for delete_template - resets to default."""
    return delete_template(workspace_id)


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════


ALLOWED_PLACEHOLDERS = {
    "price",
    "beds",
    "baths",
    "property_type",
    "city",
    "county",
    "area_text",
    "url",
    "source",
    "lease_info",
    "first_photo_url",
    "published_at",
    "updated_at",
}


def validate_template(template: str) -> list[str]:
    """
    Validate a template string.

    Returns:
        List of validation errors (empty if valid)
    """
    import re

    errors = []

    # Find all placeholders
    placeholders = set(re.findall(r"\{(\w+)\}", template))

    # Check for unknown placeholders
    unknown = placeholders - ALLOWED_PLACEHOLDERS
    if unknown:
        errors.append(f"Unknown placeholders: {', '.join(unknown)}")

    # Check for minimum required placeholders
    required = {"price", "url"}
    missing_required = required - placeholders
    if missing_required:
        errors.append(f"Missing required placeholders: {', '.join(missing_required)}")

    # Check template length
    if len(template) > 4096:
        errors.append("Template too long (max 4096 characters)")

    if len(template) < 10:
        errors.append("Template too short (min 10 characters)")

    return errors


def is_valid_template(template: str) -> bool:
    """Check if template is valid."""
    return len(validate_template(template)) == 0
