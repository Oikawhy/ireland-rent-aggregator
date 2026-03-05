"""
AGPARS Validation Module

Subscription filters, city validation, and input sanitization rules.
"""

from dataclasses import dataclass, field

from packages.observability.logger import get_logger

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# SUBSCRIPTION FILTER VALIDATION (T017)
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class SubscriptionFilters:
    """Validated subscription filter configuration."""

    min_price: int | None = None
    max_price: int | None = None
    min_beds: int | None = None
    max_beds: int | None = None
    counties: list[str] = field(default_factory=list)
    cities: list[str] = field(default_factory=list)
    property_types: list[str] = field(default_factory=list)
    furnished_only: bool = False
    exclude_shared: bool = False
    sources: list[str] = field(default_factory=list)  # Empty = all sources

    def matches(self, listing: dict) -> bool:
        """Check if a listing matches these filters."""
        # Price filter
        price = listing.get("price")
        if price is not None:
            if self.min_price and price < self.min_price:
                return False
            if self.max_price and price > self.max_price:
                return False

        # Beds filter
        beds = listing.get("beds")
        if beds is not None:
            if self.min_beds and beds < self.min_beds:
                return False
            if self.max_beds and beds > self.max_beds:
                return False

        # County filter
        if self.counties:
            county = listing.get("county", "").lower()
            if county and county not in [c.lower() for c in self.counties]:
                return False

        # City filter
        if self.cities:
            city = listing.get("city", "").lower()
            if city and city not in [c.lower() for c in self.cities]:
                return False

        # Property type filter
        if self.property_types:
            prop_type = listing.get("property_type", "").lower()
            if prop_type and prop_type not in [p.lower() for p in self.property_types]:
                return False

        # Furnished filter
        if self.furnished_only and not listing.get("furnished"):
            return False

        # Source filter
        if self.sources:
            source = listing.get("source", "").lower()
            if source and source not in [s.lower() for s in self.sources]:
                return False

        return True


def validate_subscription_filters(raw_filters: dict) -> tuple[SubscriptionFilters, list[str]]:
    """
    Validate and normalize subscription filter input.

    Returns:
        Tuple of (validated filters, list of validation errors)
    """
    errors = []
    filters = SubscriptionFilters()

    # Price validation
    if "min_price" in raw_filters:
        try:
            filters.min_price = int(raw_filters["min_price"])
            if filters.min_price < 0:
                errors.append("min_price must be positive")
        except (ValueError, TypeError):
            errors.append("min_price must be a number")

    if "max_price" in raw_filters:
        try:
            filters.max_price = int(raw_filters["max_price"])
            if filters.max_price < 0:
                errors.append("max_price must be positive")
        except (ValueError, TypeError):
            errors.append("max_price must be a number")

    if filters.min_price and filters.max_price and filters.min_price > filters.max_price:
        errors.append("min_price cannot exceed max_price")

    # Beds validation
    if "min_beds" in raw_filters:
        try:
            filters.min_beds = int(raw_filters["min_beds"])
            if filters.min_beds < 0 or filters.min_beds > 10:
                errors.append("min_beds must be between 0 and 10")
        except (ValueError, TypeError):
            errors.append("min_beds must be a number")

    if "max_beds" in raw_filters:
        try:
            filters.max_beds = int(raw_filters["max_beds"])
            if filters.max_beds < 0 or filters.max_beds > 10:
                errors.append("max_beds must be between 0 and 10")
        except (ValueError, TypeError):
            errors.append("max_beds must be a number")

    # List fields
    if "counties" in raw_filters:
        if isinstance(raw_filters["counties"], list):
            filters.counties = [str(c).strip() for c in raw_filters["counties"] if c]
        else:
            errors.append("counties must be a list")

    if "cities" in raw_filters:
        if isinstance(raw_filters["cities"], list):
            filters.cities = [str(c).strip() for c in raw_filters["cities"] if c]
        else:
            errors.append("cities must be a list")

    if "property_types" in raw_filters:
        valid_types = ["apartment", "house", "studio", "other"]
        if isinstance(raw_filters["property_types"], list):
            for pt in raw_filters["property_types"]:
                if pt.lower() in valid_types:
                    filters.property_types.append(pt.lower())
                else:
                    errors.append(f"Invalid property_type: {pt}")
        else:
            errors.append("property_types must be a list")

    # Boolean fields
    filters.furnished_only = bool(raw_filters.get("furnished_only", False))
    filters.exclude_shared = bool(raw_filters.get("exclude_shared", False))

    return filters, errors


# ═══════════════════════════════════════════════════════════════════════════════
# DIGEST RULES VALIDATION (per ARCHITECT.md)
# ═══════════════════════════════════════════════════════════════════════════════

# Allowed digest intervals per ARCHITECT.md
ALLOWED_DIGEST_INTERVALS = ["once_daily", "twice_daily", "weekly"]


@dataclass
class DigestSchedule:
    """
    Validated digest schedule configuration.

    Format per ARCHITECT.md:
    { interval: string, times: string[] }
    e.g., { interval: "twice_daily", times: ["09:00", "18:00"] }
    """
    interval: str = "once_daily"
    times: list[str] = field(default_factory=lambda: ["09:00"])
    timezone: str = "Europe/Dublin"


def _parse_time(time_str: str) -> tuple[int, int] | None:
    """Parse HH:MM format to (hour, minute) tuple."""
    try:
        parts = time_str.strip().split(":")
        if len(parts) != 2:
            return None
        hour = int(parts[0])
        minute = int(parts[1])
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return (hour, minute)
        return None
    except (ValueError, AttributeError):
        return None


def _minutes_between(time1: str, time2: str) -> int:
    """Calculate minutes between two times."""
    t1 = _parse_time(time1)
    t2 = _parse_time(time2)
    if not t1 or not t2:
        return 0

    mins1 = t1[0] * 60 + t1[1]
    mins2 = t2[0] * 60 + t2[1]

    diff = abs(mins2 - mins1)
    # Handle wrap-around (e.g., 23:00 and 01:00)
    return min(diff, 1440 - diff)


def validate_digest_schedule(raw_schedule: dict) -> tuple[DigestSchedule, list[str]]:
    """
    Validate digest schedule configuration per ARCHITECT.md.

    Requirements:
    - interval: one of 'once_daily', 'twice_daily', 'weekly'
    - times: list of HH:MM strings
    - Times must be at least 4 hours apart
    """
    errors = []
    schedule = DigestSchedule()

    # Validate interval
    interval = raw_schedule.get("interval", "once_daily")
    if interval not in ALLOWED_DIGEST_INTERVALS:
        errors.append(f"Invalid interval '{interval}'. Must be one of: {ALLOWED_DIGEST_INTERVALS}")
    else:
        schedule.interval = interval

    # Validate times
    times = raw_schedule.get("times", [])
    if not isinstance(times, list):
        errors.append("times must be a list")
        times = []

    validated_times = []
    for t in times:
        parsed = _parse_time(t)
        if parsed:
            validated_times.append(t)
        else:
            errors.append(f"Invalid time format '{t}'. Must be HH:MM")

    if not validated_times:
        validated_times = ["09:00"]  # Default

    schedule.times = validated_times

    # Validate 4-hour minimum gap between times
    if len(validated_times) >= 2:
        sorted_times = sorted(validated_times, key=lambda t: _parse_time(t) or (0, 0))
        for i in range(len(sorted_times) - 1):
            gap = _minutes_between(sorted_times[i], sorted_times[i + 1])
            if gap < 240:  # 4 hours = 240 minutes
                errors.append(
                    f"Times {sorted_times[i]} and {sorted_times[i + 1]} must be at least 4 hours apart"
                )

    # Validate timezone if provided
    if "timezone" in raw_schedule:
        schedule.timezone = str(raw_schedule["timezone"])

    return schedule, errors


# ═══════════════════════════════════════════════════════════════════════════════
# CITY VALIDATION (T025)
# ═══════════════════════════════════════════════════════════════════════════════

# Valid Irish counties
VALID_COUNTIES = [
    "Carlow", "Cavan", "Clare", "Cork", "Donegal", "Dublin", "Galway",
    "Kerry", "Kildare", "Kilkenny", "Laois", "Leitrim", "Limerick",
    "Longford", "Louth", "Mayo", "Meath", "Monaghan", "Offaly",
    "Roscommon", "Sligo", "Tipperary", "Waterford", "Westmeath",
    "Wexford", "Wicklow",
]


def is_valid_county(county: str) -> bool:
    """Check if a county name is valid."""
    return county.title() in VALID_COUNTIES


def normalize_county(county: str) -> str | None:
    """Normalize county name to standard format."""
    normalized = county.strip().title()
    if normalized in VALID_COUNTIES:
        return normalized
    # Handle common prefixes
    if normalized.startswith("Co. "):
        normalized = normalized[4:]
    if normalized.startswith("County "):
        normalized = normalized[7:]
    if normalized in VALID_COUNTIES:
        return normalized
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# CITY VALIDATION AGAINST DB (T025)
# ═══════════════════════════════════════════════════════════════════════════════


def validate_city_ids(city_ids: list[int]) -> tuple[list[int], list[int]]:
    """
    Validate city IDs against core.cities table.

    Args:
        city_ids: List of city IDs to validate

    Returns:
        Tuple of (valid_ids, invalid_ids)
    """
    if not city_ids:
        return [], []

    try:
        from sqlalchemy import text

        from packages.storage.db import get_readonly_session

        with get_readonly_session() as session:
            result = session.execute(
                text("SELECT id FROM core.cities WHERE id = ANY(:ids)"),
                {"ids": city_ids}
            )
            valid_ids = [row[0] for row in result.fetchall()]

        invalid_ids = [cid for cid in city_ids if cid not in valid_ids]
        return valid_ids, invalid_ids

    except Exception as e:
        logger.error("City validation failed", error=str(e))
        # Return all as valid if DB not available
        return city_ids, []


def get_city_by_name(name: str) -> dict | None:
    """
    Get city from DB by name (case-insensitive, with synonyms).

    Args:
        name: City name to search

    Returns:
        City dict or None
    """
    if not name:
        return None

    try:
        from sqlalchemy import text

        from packages.storage.db import get_readonly_session

        normalized_name = name.strip().lower()

        with get_readonly_session() as session:
            # Check exact match first
            result = session.execute(
                text("""
                    SELECT id, name, county, population
                    FROM core.cities
                    WHERE LOWER(name) = :name
                    LIMIT 1
                """),
                {"name": normalized_name}
            )
            row = result.fetchone()

            if row:
                return {
                    "id": row[0],
                    "name": row[1],
                    "county": row[2],
                    "population": row[3],
                }

            # Check synonyms
            result = session.execute(
                text("""
                    SELECT id, name, county, population
                    FROM core.cities
                    WHERE :name = ANY(synonyms)
                    LIMIT 1
                """),
                {"name": normalized_name}
            )
            row = result.fetchone()

            if row:
                return {
                    "id": row[0],
                    "name": row[1],
                    "county": row[2],
                    "population": row[3],
                }

        return None

    except Exception as e:
        logger.error("City lookup failed", error=str(e))
        return None
