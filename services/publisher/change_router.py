"""
AGPARS Change Router

Routes NEW/UPDATED listing events to subscriptions.
"""

from dataclasses import dataclass

from packages.observability.logger import get_logger
from packages.storage.subscriptions import get_active_subscriptions
from services.normalizer.change_detector import ChangeEvent, ChangeType
from services.rules.subscription_filters import matches_subscription

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTING RESULT
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class RoutingResult:
    """Result of routing a listing to subscriptions."""

    listing_id: int
    change_type: ChangeType
    subscription_ids: list[int]
    workspace_ids: list[int]
    # Mapping: subscription_id -> workspace_id for correct event creation
    subscription_workspaces: dict[int, int]
    # Mapping: subscription_id -> delivery_mode (instant/digest/paused)
    subscription_delivery_modes: dict[int, str]


# ═══════════════════════════════════════════════════════════════════════════════
# CHANGE ROUTER
# ═══════════════════════════════════════════════════════════════════════════════


class ChangeRouter:
    """
    Routes listing changes to matching subscriptions.

    For each NEW or UPDATED listing:
    1. Find all active subscriptions
    2. Match listing against each subscription's filters
    3. Group by workspace for delivery
    """

    def __init__(self):
        self.logger = get_logger(__name__)
        self._subscription_cache: list[dict] | None = None
        self._cache_time: float = 0

    def route_listing(
        self,
        listing: dict,
        change_event: ChangeEvent,
    ) -> RoutingResult:
        """
        Route a listing change to matching subscriptions.

        Args:
            listing: Normalized listing dict
            change_event: The detected change

        Returns:
            RoutingResult with matching subscription/workspace IDs
        """
        # Get active subscriptions (cached)
        subscriptions = self._get_subscriptions()

        matching_sub_ids = []
        matching_workspace_ids = set()
        subscription_workspaces: dict[int, int] = {}

        subscription_delivery_modes: dict[int, str] = {}

        for sub in subscriptions:
            if matches_subscription(listing, sub):
                sub_id = sub["id"]
                workspace_id = sub["workspace_id"]
                matching_sub_ids.append(sub_id)
                matching_workspace_ids.add(workspace_id)
                subscription_workspaces[sub_id] = workspace_id
                subscription_delivery_modes[sub_id] = sub.get("delivery_mode", "instant")

        result = RoutingResult(
            listing_id=listing.get("id", 0),
            change_type=change_event.change_type,
            subscription_ids=matching_sub_ids,
            workspace_ids=list(matching_workspace_ids),
            subscription_workspaces=subscription_workspaces,
            subscription_delivery_modes=subscription_delivery_modes,
        )

        self.logger.debug(
            "Routed listing",
            listing_id=result.listing_id,
            change_type=result.change_type.value,
            subscriptions=len(matching_sub_ids),
            workspaces=len(matching_workspace_ids),
        )

        return result

    def route_batch(
        self,
        listings: list[dict],
        change_events: list[ChangeEvent],
    ) -> list[RoutingResult]:
        """
        Route a batch of listings.

        Args:
            listings: List of normalized listings
            change_events: Corresponding change events

        Returns:
            List of RoutingResults
        """
        if len(listings) != len(change_events):
            raise ValueError("Listings and events must have same length")

        results = []
        for listing, event in zip(listings, change_events, strict=False):
            result = self.route_listing(listing, event)
            results.append(result)

        return results

    def _get_subscriptions(self) -> list[dict]:
        """Get subscriptions with caching."""
        import time

        now = time.time()
        cache_ttl = 60  # 1 minute

        if (
            self._subscription_cache is None
            or now - self._cache_time > cache_ttl
        ):
            self._subscription_cache = get_active_subscriptions()
            self._cache_time = now

        return self._subscription_cache or []

    def clear_cache(self) -> None:
        """Clear subscription cache."""
        self._subscription_cache = None
        self._cache_time = 0


# ═══════════════════════════════════════════════════════════════════════════════
# CONVENIENCE FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════


_router: ChangeRouter | None = None


def get_router() -> ChangeRouter:
    """Get global router instance."""
    global _router
    if _router is None:
        _router = ChangeRouter()
    return _router


def route_listing(listing: dict, change_event: ChangeEvent) -> RoutingResult:
    """Route a single listing change."""
    return get_router().route_listing(listing, change_event)


def route_batch(
    listings: list[dict],
    change_events: list[ChangeEvent],
) -> list[RoutingResult]:
    """Route a batch of listings."""
    return get_router().route_batch(listings, change_events)


def get_routing_stats(results: list[RoutingResult]) -> dict:
    """Get statistics from routing results."""
    total = len(results)
    routed = sum(1 for r in results if r.subscription_ids)
    unrouted = total - routed

    workspace_counts: dict[int, int] = {}
    for result in results:
        for ws_id in result.workspace_ids:
            workspace_counts[ws_id] = workspace_counts.get(ws_id, 0) + 1

    return {
        "total_listings": total,
        "routed": routed,
        "unrouted": unrouted,
        "unique_workspaces": len(workspace_counts),
        "listings_per_workspace": workspace_counts,
    }
