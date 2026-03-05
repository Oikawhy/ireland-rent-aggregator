"""
AGPARS Collector Adapters Package

Per-source scraping adapters for Irish rental websites.
"""

from services.collector.adapters.daft import DaftAdapter
from services.collector.adapters.dng import DngAdapter
from services.collector.adapters.myhome import MyHomeAdapter
from services.collector.adapters.property_ie import PropertyIeAdapter
from services.collector.adapters.rent import RentAdapter
from services.collector.adapters.sherryfitz import SherryFitzAdapter

__all__ = [
    "DaftAdapter",
    "RentAdapter",
    "MyHomeAdapter",
    "PropertyIeAdapter",
    "SherryFitzAdapter",
    "DngAdapter",
    "ADAPTERS",
    "get_adapter",
]


# Adapter registry mapping source names to adapter classes
ADAPTERS = {
    "daft": DaftAdapter,
    "rent": RentAdapter,
    "myhome": MyHomeAdapter,
    "property": PropertyIeAdapter,
    "sherryfitz": SherryFitzAdapter,
    "dng": DngAdapter,
}


def get_adapter(source: str):
    """
    Get an adapter instance by source name.

    Args:
        source: Source name (e.g., 'daft', 'rent')

    Returns:
        Adapter instance or None if not found
    """
    adapter_class = ADAPTERS.get(source)
    if adapter_class:
        return adapter_class()
    return None
