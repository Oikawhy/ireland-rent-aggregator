"""
AGPARS Extended Keyword Dictionaries

Exclusion keywords for filtering out student housing,
short-term rentals, and Northern Ireland listings.

Covers T080.
"""

# ═══════════════════════════════════════════════════════════════════════════════
# STUDENT HOUSING KEYWORDS
# ═══════════════════════════════════════════════════════════════════════════════

STUDENT_KEYWORDS: list[str] = [
    "student accommodation",
    "student housing",
    "student letting",
    "student room",
    "student share",
    "campus accommodation",
    "campus living",
    "college accommodation",
    "university accommodation",
    "digs",
    "student digs",
    "student only",
    "students only",
    "term time",
    "academic year",
    "semester let",
]


# ═══════════════════════════════════════════════════════════════════════════════
# SHORT-TERM RENTAL KEYWORDS
# ═══════════════════════════════════════════════════════════════════════════════

SHORT_TERM_KEYWORDS: list[str] = [
    "short term",
    "short-term",
    "holiday let",
    "holiday rental",
    "holiday home",
    "airbnb",
    "serviced apartment",
    "temporary accommodation",
    "corporate let",
    "nightly rate",
    "per night",
]


# ═══════════════════════════════════════════════════════════════════════════════
# NORTHERN IRELAND / UK INDICATORS
# ═══════════════════════════════════════════════════════════════════════════════

NORTHERN_IRELAND_COUNTIES: list[str] = [
    "antrim",
    "armagh",
    "down",
    "fermanagh",
    "londonderry",
    "derry",
    "tyrone",
]

NORTHERN_IRELAND_CITIES: list[str] = [
    "belfast",
    "newry",
    "lisburn",
    "bangor",
    "craigavon",
    "newtownabbey",
    "ballymena",
    "newtownards",
    "carrickfergus",
    "coleraine",
    "omagh",
    "enniskillen",
    "strabane",
    "cookstown",
    "dungannon",
    "larne",
    "limavady",
    "downpatrick",
    "portadown",
    "lurgan",
]

# BT postcodes (Northern Ireland)
NI_POSTCODE_PREFIX = "BT"


# ═══════════════════════════════════════════════════════════════════════════════
# MATCH HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def matches_student_keywords(text: str) -> bool:
    """Check if text contains student housing keywords."""
    lower = text.lower()
    return any(kw in lower for kw in STUDENT_KEYWORDS)


def matches_short_term_keywords(text: str) -> bool:
    """Check if text contains short-term rental keywords."""
    lower = text.lower()
    return any(kw in lower for kw in SHORT_TERM_KEYWORDS)


def is_northern_ireland(location: str) -> bool:
    """Check if location is in Northern Ireland."""
    lower = location.lower()

    if any(county in lower for county in NORTHERN_IRELAND_COUNTIES):
        return True
    if any(city in lower for city in NORTHERN_IRELAND_CITIES):
        return True
    if lower.startswith(NI_POSTCODE_PREFIX.lower()):
        return True

    return False


def should_exclude(listing: dict) -> str | None:
    """
    Check if a listing should be excluded.

    Returns:
        Exclusion reason string, or None if listing is OK
    """
    title = listing.get("title", "")
    description = listing.get("description", "")
    text = f"{title} {description}"

    if matches_student_keywords(text):
        return "student_housing"

    if matches_short_term_keywords(text):
        return "short_term_rental"

    location = listing.get("area_text", "")
    county = listing.get("county", "")
    full_location = f"{location} {county}"

    if is_northern_ireland(full_location):
        return "northern_ireland"

    return None
