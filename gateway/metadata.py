"""
Index metadata cache — resolves concrete indices to their logical group
(alias or data stream).

Periodically fetches alias and data stream mappings from Elasticsearch
and exposes a synchronous `resolve_group()` lookup for use at event-emission time.
"""

from __future__ import annotations
import asyncio
import logging

import httpx

from config import ES_HOST, EVENT_TIMEOUT, METADATA_REFRESH_INTERVAL

logger = logging.getLogger(__name__)

_client = httpx.AsyncClient(base_url=ES_HOST, timeout=EVENT_TIMEOUT)

# Internal lookup: concrete index name → group name
_index_to_group: dict[str, str] = {}

# Groups listing: group name → set of concrete index names
_groups: dict[str, set[str]] = {}


async def refresh() -> None:
    """Fetch alias and data stream mappings from ES and rebuild the lookup.

    Data streams take priority over aliases — if an index belongs to both,
    the data stream wins. The global state (_index_to_group, _groups) is
    swapped atomically via Python reference assignment (safe under asyncio's
    single-threaded model).
    """
    new_lookup: dict[str, str] = {}
    new_groups: dict[str, set[str]] = {}

    # --- Aliases ---
    try:
        resp = await _client.get("/_aliases")
        if resp.status_code == 200:
            data = resp.json()
            # Response format: {"index_name": {"aliases": {"alias_name": {}, ...}}, ...}
            for index_name, index_info in data.items():
                if index_name.startswith("."):
                    continue  # skip system indices
                aliases = list(index_info.get("aliases", {}).keys())
                if aliases:
                    # Use the first alias as the group (most indices have one alias)
                    group = aliases[0]
                    new_lookup[index_name] = group
                    new_groups.setdefault(group, set()).add(index_name)
                else:
                    # No alias — index is its own group
                    new_lookup[index_name] = index_name
                    new_groups.setdefault(index_name, set()).add(index_name)
        else:
            logger.warning("Failed to fetch aliases: %s", resp.status_code)
    except httpx.RequestError as exc:
        logger.warning("Failed to fetch aliases: %s", exc)

    # --- Data streams (override aliases if present) ---
    try:
        resp = await _client.get("/_data_stream/*")
        if resp.status_code == 200:
            data = resp.json()
            for ds in data.get("data_streams", []):
                ds_name = ds["name"]
                for idx_info in ds.get("indices", []):
                    idx_name = idx_info["index_name"]
                    # Data stream takes priority over alias
                    old_group = new_lookup.get(idx_name)
                    if old_group and old_group != idx_name:
                        # Remove from old alias group
                        if old_group in new_groups:
                            new_groups[old_group].discard(idx_name)
                            if not new_groups[old_group]:
                                del new_groups[old_group]
                    new_lookup[idx_name] = ds_name
                    new_groups.setdefault(ds_name, set()).add(idx_name)
        # 404 is fine — no data streams configured
        elif resp.status_code != 404:
            logger.warning("Failed to fetch data streams: %s", resp.status_code)
    except httpx.RequestError as exc:
        logger.warning("Failed to fetch data streams: %s", exc)

    global _index_to_group, _groups
    _index_to_group = new_lookup
    _groups = new_groups

    logger.info(
        "Metadata refreshed: %d concrete indices → %d groups",
        len(new_lookup), len(new_groups),
    )


def resolve_group(index_name: str) -> str:
    """Map a concrete index (or alias) name to its logical group.

    Falls back to returning the index name itself if not found in the cache.
    This also handles the case where queries target the alias directly
    (e.g., path=/logs/_search → index_name="logs").
    """
    # Direct match (concrete index → group)
    if index_name in _index_to_group:
        return _index_to_group[index_name]

    # Check if the name IS a group (alias queried directly)
    if index_name in _groups:
        return index_name

    # Fallback — index is its own group
    return index_name


def get_groups() -> dict[str, list[str]]:
    """Return the current groups mapping (group_name → concrete indices)."""
    return {k: sorted(v) for k, v in _groups.items()}


async def _refresh_loop() -> None:
    """Background loop that refreshes metadata periodically."""
    while True:
        try:
            await refresh()
        except Exception:
            logger.exception("Metadata refresh failed")
        await asyncio.sleep(METADATA_REFRESH_INTERVAL)


def start_refresh_loop() -> None:
    """Start the background metadata refresh loop. Call from within an async context."""
    loop = asyncio.get_running_loop()
    loop.create_task(_refresh_loop())
    logger.info("Metadata refresh loop started (interval=%ds)", METADATA_REFRESH_INTERVAL)
