"""
AGPARS Scraper Fixtures

T034 - Test fixtures for scraper testing.
Provides mock data, HTML samples, and test utilities.
"""


import pytest

# ═══════════════════════════════════════════════════════════════════════════════
# SAMPLE HTML FIXTURES
# ═══════════════════════════════════════════════════════════════════════════════


SAMPLE_LISTING_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>2 Bed Apartment, Dublin 4</title>
    <script type="application/ld+json">
    {
        "@context": "https://schema.org",
        "@type": "RealEstateListing",
        "name": "2 Bed Apartment, Dublin 4",
        "description": "Lovely 2 bedroom apartment in prime Dublin 4 location",
        "offers": {
            "@type": "Offer",
            "price": "1800",
            "priceCurrency": "EUR"
        }
    }
    </script>
</head>
<body>
    <div class="listing">
        <h1>2 Bed Apartment, Dublin 4</h1>
        <span class="price">€1,800 per month</span>
        <span class="beds">2 beds</span>
        <span class="baths">1 bath</span>
        <p class="description">Lovely 2 bedroom apartment in prime Dublin 4 location</p>
    </div>
</body>
</html>
"""

SAMPLE_LISTING_PAGE_EMPTY = """
<!DOCTYPE html>
<html>
<head><title>No Listings</title></head>
<body>
    <div class="no-results">No properties found matching your criteria</div>
</body>
</html>
"""

SAMPLE_LISTING_PAGE_MULTIPLE = """
<!DOCTYPE html>
<html>
<body>
    <div class="listing" data-id="1">
        <a href="/property/1">Property 1</a>
        <span class="price">€1,500</span>
    </div>
    <div class="listing" data-id="2">
        <a href="/property/2">Property 2</a>
        <span class="price">€1,800</span>
    </div>
    <div class="listing" data-id="3">
        <a href="/property/3">Property 3</a>
        <span class="price">€2,200</span>
    </div>
</body>
</html>
"""


# ═══════════════════════════════════════════════════════════════════════════════
# RAW LISTING FIXTURES
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.fixture
def sample_raw_listing():
    """Create a sample raw listing dict."""
    return {
        "source": "daft",
        "source_listing_id": "12345",
        "url": "https://daft.ie/property/12345",
        "title": "2 Bed Apartment, Dublin 4",
        "price_text": "€1,800 per month",
        "beds_text": "2",
        "baths_text": "1",
        "location_text": "Dublin 4",
        "property_type_text": "Apartment",
        "first_photo_url": "https://daft.ie/images/12345.jpg",
    }


@pytest.fixture
def sample_raw_listings():
    """Create multiple sample raw listings."""
    return [
        {
            "source": "daft",
            "source_listing_id": "1",
            "url": "https://daft.ie/1",
            "price_text": "€1,500",
            "beds_text": "1",
        },
        {
            "source": "daft",
            "source_listing_id": "2",
            "url": "https://daft.ie/2",
            "price_text": "€1,800",
            "beds_text": "2",
        },
        {
            "source": "daft",
            "source_listing_id": "3",
            "url": "https://daft.ie/3",
            "price_text": "€2,200",
            "beds_text": "3",
        },
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# NORMALIZED LISTING FIXTURES
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.fixture
def sample_normalized_listing():
    """Create a sample normalized listing dict."""
    return {
        "raw_id": 1,
        "price": 1800,
        "beds": 2,
        "baths": 1,
        "property_type": "apartment",
        "furnished": True,
        "city_id": 1,  # Dublin
        "county": "Dublin",
        "area_text": "Dublin 4",
        "lease_length_months": 12,
        "lease_length_unknown": False,
        "status": "active",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SCRAPE JOB FIXTURES
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.fixture
def sample_scrape_job():
    """Create a sample scrape job."""
    from services.collector.runner import ScrapeJob

    return ScrapeJob(
        source="daft",
        city="Dublin",
        county="Dublin",
        city_id=1,
    )


@pytest.fixture
def sample_scrape_jobs():
    """Create multiple scrape jobs for different sources."""
    from services.collector.runner import ScrapeJob

    return [
        ScrapeJob(source="daft", city="Dublin", county="Dublin"),
        ScrapeJob(source="rent", city="Cork", county="Cork"),
        ScrapeJob(source="myhome", city="Galway", county="Galway"),
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE CONFIG FIXTURES
# ═══════════════════════════════════════════════════════════════════════════════


SOURCES = ["daft", "rent", "myhome", "property", "sherryfitz", "dng"]


@pytest.fixture
def all_sources():
    """List of all supported sources."""
    return SOURCES


# ═══════════════════════════════════════════════════════════════════════════════
# EXCLUSION TEST DATA
# ═══════════════════════════════════════════════════════════════════════════════


STUDENT_LISTINGS = [
    {"title": "Student Accommodation near UCD", "description": ""},
    {"title": "Campus Living", "description": "Perfect for students"},
    {"title": "Apartment", "description": "Student lets only"},
]

SHORT_TERM_LISTINGS = [
    {"title": "Short Stay Dublin", "description": "1 month minimum"},
    {"title": "Apartment", "description": "Available for 6 weeks"},
    {"title": "Holiday Let", "description": "Short term rental"},
]

NORTHERN_IRELAND_LISTINGS = [
    {"title": "Belfast City Centre", "location": "Belfast"},
    {"title": "Apartment", "location": "Derry"},
    {"title": "House", "location": "Newry, Northern Ireland"},
]


@pytest.fixture
def student_listing_samples():
    """Listings that should be excluded as student housing."""
    return STUDENT_LISTINGS


@pytest.fixture
def short_term_listing_samples():
    """Listings that should be excluded as short-term."""
    return SHORT_TERM_LISTINGS


@pytest.fixture
def northern_ireland_listing_samples():
    """Listings that should be excluded as Northern Ireland."""
    return NORTHERN_IRELAND_LISTINGS
