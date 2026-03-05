"""
AGPARS Bot Input Validation

External input validation for Telegram bot commands.
"""

import re

from packages.observability.logger import get_logger

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════

MAX_SUBSCRIPTION_NAME_LENGTH = 100
MAX_CITIES_PER_SUBSCRIPTION = 20
MAX_PRICE = 50000
MIN_PRICE = 0
MAX_BEDS = 10

# Regex patterns
CHAT_ID_PATTERN = re.compile(r"^-?\d+$")
USERNAME_PATTERN = re.compile(r"^@?[a-zA-Z][a-zA-Z0-9_]{4,31}$")


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATION FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════


def validate_chat_id(chat_id: str | int) -> tuple[bool, int | None, str | None]:
    """
    Validate a Telegram chat ID.

    Returns:
        Tuple of (is_valid, parsed_id, error_message)
    """
    try:
        if isinstance(chat_id, int):
            return True, chat_id, None
        if CHAT_ID_PATTERN.match(str(chat_id)):
            return True, int(chat_id), None
        return False, None, "Invalid chat ID format"
    except (ValueError, TypeError):
        return False, None, "Chat ID must be a number"


def validate_subscription_name(name: str) -> tuple[bool, str, str | None]:
    """
    Validate and sanitize a subscription name.

    Returns:
        Tuple of (is_valid, sanitized_name, error_message)
    """
    if not name:
        return False, "", "Subscription name is required"

    # Strip and limit length
    sanitized = name.strip()[:MAX_SUBSCRIPTION_NAME_LENGTH]

    # Check for forbidden characters
    if re.search(r'[<>"\'\\/]', sanitized):
        return False, "", "Name contains forbidden characters"

    if len(sanitized) < 2:
        return False, "", "Name must be at least 2 characters"

    return True, sanitized, None


def validate_price_input(value: str) -> tuple[bool, int | None, str | None]:
    """
    Validate price input from user.

    Returns:
        Tuple of (is_valid, parsed_value, error_message)
    """
    try:
        # Remove currency symbols and spaces
        cleaned = re.sub(r"[€$,\s]", "", value)
        price = int(cleaned)

        if price < MIN_PRICE:
            return False, None, "Price cannot be negative"
        if price > MAX_PRICE:
            return False, None, f"Price cannot exceed €{MAX_PRICE:,}"

        return True, price, None
    except (ValueError, TypeError):
        return False, None, "Invalid price format. Use numbers only."


def validate_beds_input(value: str) -> tuple[bool, int | None, str | None]:
    """
    Validate beds input from user.

    Returns:
        Tuple of (is_valid, parsed_value, error_message)
    """
    try:
        beds = int(value.strip())

        if beds < 0:
            return False, None, "Beds cannot be negative"
        if beds > MAX_BEDS:
            return False, None, f"Beds cannot exceed {MAX_BEDS}"

        return True, beds, None
    except (ValueError, TypeError):
        return False, None, "Invalid beds format. Use a number."


def validate_city_list(cities: list[str]) -> tuple[bool, list[str], str | None]:
    """
    Validate a list of city names.

    Returns:
        Tuple of (is_valid, sanitized_list, error_message)
    """
    if not cities:
        return True, [], None

    if len(cities) > MAX_CITIES_PER_SUBSCRIPTION:
        return False, [], f"Maximum {MAX_CITIES_PER_SUBSCRIPTION} cities allowed"

    sanitized = []
    for city in cities:
        clean = city.strip().title()
        if len(clean) > 100:
            return False, [], f"City name too long: {city[:20]}..."
        if clean:
            sanitized.append(clean)

    return True, sanitized, None


def validate_username(username: str) -> tuple[bool, str, str | None]:
    """
    Validate a Telegram username.

    Returns:
        Tuple of (is_valid, normalized_username, error_message)
    """
    if not username:
        return False, "", "Username is required"

    # Normalize: ensure @ prefix
    normalized = username.strip()
    if not normalized.startswith("@"):
        normalized = f"@{normalized}"

    if not USERNAME_PATTERN.match(normalized):
        return False, "", "Invalid username format"

    return True, normalized, None


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND PARSING
# ═══════════════════════════════════════════════════════════════════════════════


def parse_filter_command(text: str) -> dict:
    """
    Parse a filter command string like:
    "/filter price:500-2000 beds:2+ county:dublin"

    Returns:
        Dict of parsed filter parameters
    """
    filters = {}
    parts = text.split()

    for part in parts[1:]:  # Skip command itself
        if ":" not in part:
            continue

        key, value = part.split(":", 1)
        key = key.lower().strip()
        value = value.strip()

        if key == "price":
            if "-" in value:
                min_p, max_p = value.split("-", 1)
                if min_p:
                    filters["min_price"] = min_p
                if max_p:
                    filters["max_price"] = max_p
            elif value.endswith("+"):
                filters["min_price"] = value[:-1]
            elif value.endswith("-"):
                filters["max_price"] = value[:-1]
            else:
                filters["max_price"] = value

        elif key == "beds":
            if value.endswith("+"):
                filters["min_beds"] = value[:-1]
            elif value.endswith("-"):
                filters["max_beds"] = value[:-1]
            else:
                filters["beds"] = value

        elif key in ("county", "counties"):
            filters["counties"] = [c.strip() for c in value.split(",")]

        elif key in ("city", "cities"):
            filters["cities"] = [c.strip() for c in value.split(",")]

        elif key == "type":
            filters["property_types"] = [t.strip() for t in value.split(",")]

    return filters
