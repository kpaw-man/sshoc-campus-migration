import json
import logging
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

V1_BASE = "https://campus.dariah.eu/api/v1/metadata/resources"
V2_BASE = "https://campus.dariah.eu/api/v2/metadata/resources"
OUTPUT_FILE = "/data/dariah_resources.json"
PAGE_SIZE = 100

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


def extract_items(data: Any) -> List[Dict[str, Any]]:
    """Extract a list of records from an API response."""
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]

    if isinstance(data, dict):
        for key in ("resources", "data", "results", "items", "content"):
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return [data]

    return []


def fetch_all_offset_pagination(
    base_url: str,
    fields: List[str],
    page_size: int = PAGE_SIZE,
) -> Dict[str, Dict[str, Any]]:
    """
    Fetch all entries from an offset-paginated endpoint.
    Returns a dict keyed by item id: {"123": {"field": value, ...}}
    """
    results: Dict[str, Dict[str, Any]] = {}
    seen_offsets: Set[int] = set()
    offset = 0
    duplicate_ids = 0

    with requests.Session() as session:
        while True:
            if offset in seen_offsets:
                raise RuntimeError(f"Pagination loop detected at offset={offset}")
            seen_offsets.add(offset)

            resp = session.get(base_url, params={"offset": offset, "limit": page_size}, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            items = extract_items(data)

            if not items:
                log.info("Empty response — stopping pagination.")
                break

            current_offset = offset
            returned_limit = page_size
            total: Optional[int] = None

            if isinstance(data, dict):
                current_offset = int(data.get("offset", offset) or 0)
                returned_limit = int(data.get("limit", page_size) or page_size)
                if (total_raw := data.get("total")) is not None:
                    total = int(total_raw)

            for item in items:
                if (item_id := item.get("id")) is None:
                    continue
                item_id = str(item_id)
                if item_id in results:
                    duplicate_ids += 1
                results[item_id] = {field: item.get(field) for field in fields}

            step = returned_limit or len(items)
            if step <= 0:
                log.info("Invalid pagination step (%d) — stopping.", step)
                break

            next_offset = current_offset + step

            if total is not None:
                log.info("offset=%d, limit=%d, fetched=%d, total=%d",
                         current_offset, returned_limit, len(items), total)
                if next_offset >= total:
                    break
            elif len(items) < step:
                break

            offset = next_offset

    log.info("Total fetched: %d entries (%d duplicate ids overwritten).",
             len(results), duplicate_ids)
    return results


def fetch_v1_all() -> Dict[str, Dict[str, Any]]:
    """Fetch all entries from API v1. Returns {id: {'uuid': ...}}"""
    return fetch_all_offset_pagination(V1_BASE, fields=["uuid"])


def fetch_v2_all() -> Dict[str, Dict[str, Any]]:
    """Fetch all entries from API v2. Returns {id: {'pid': ..., 'title': ...}}"""
    return fetch_all_offset_pagination(V2_BASE, fields=["pid", "title"])


def deduplicate(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove duplicates by item id."""
    seen: Set[Any] = set()
    unique = []
    for item in items:
        if (item_id := item.get("id")) not in seen:
            seen.add(item_id)
            unique.append(item)
    return unique


def _sort_key(value: str) -> Tuple[int, Any]:
    return (0, int(value)) if value.isdigit() else (1, value)


def merge_and_save(
    v1_data: Dict[str, Dict[str, Any]],
    v2_data: Dict[str, Dict[str, Any]],
) -> None:
    """Merge v1 and v2 data by id, then save to JSON."""
    all_ids = set(v1_data.keys()) | set(v2_data.keys())

    merged = [
        {
            "id":    item_id,
            "uuid":  v1_data.get(item_id, {}).get("uuid"),
            "pid":   v2_data.get(item_id, {}).get("pid"),
            "title": v2_data.get(item_id, {}).get("title"),
        }
        for item_id in sorted(all_ids, key=_sort_key)
    ]

    only_v1 = sum(1 for e in merged if e["uuid"] and not e["pid"] and not e["title"])
    only_v2 = sum(1 for e in merged if not e["uuid"] and (e["pid"] or e["title"]))
    both    = sum(1 for e in merged if e["uuid"] and (e["pid"] or e["title"]))

    log.info("Merge stats — both: %d, v1 only: %d, v2 only: %d, total: %d",
             both, only_v1, only_v2, len(merged))

    merged = deduplicate(merged)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    log.info("Saved to: %s", OUTPUT_FILE)


def main() -> None:
    log.info("=== Step 1: Fetching data from API v1 ===")
    v1_data = fetch_v1_all()

    log.info("\n=== Step 2: Fetching data from API v2 ===")
    v2_data = fetch_v2_all()

    log.info("\n=== Step 3: Merging and saving to JSON ===")
    merge_and_save(v1_data, v2_data)


if __name__ == "__main__":
    main()