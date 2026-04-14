import json
import logging
import os
from typing import Any, Dict, List, Set

import requests

SSHOC_BASE_URL = os.environ.get("SSHOC_BASE_URL", "https://sshoc-marketplace-api.acdh-dev.oeaw.ac.at")
SOURCE_ID      = int(os.environ.get("SOURCE_ID", 253))
BASE_URL       = "%s/api/sources/%d/items" % (SSHOC_BASE_URL, SOURCE_ID)
OUTPUT_FILE    = "/data/sshoc_source_items.json"
PER_PAGE       = 100

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

ITEM_FIELDS = ("id", "category", "label", "persistentId", "lastInfoUpdate")


def fetch_page(session: requests.Session, page: int) -> Dict[str, Any]:
    """Fetch a single page of results."""
    response = session.get(BASE_URL, params={"page": page, "perpage": PER_PAGE}, timeout=30)
    response.raise_for_status()
    return response.json()


def normalize_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """Keep only required fields."""
    return {field: item.get(field) for field in ITEM_FIELDS}


def fetch_all_items() -> List[Dict[str, Any]]:
    """Fetch all pages and return a flat list of normalized items."""
    with requests.Session() as session:
        first = fetch_page(session, 1)
        total_pages = int(first.get("pages") or 1)
        log.info("hits=%s, pages=%s", first.get("hits"), total_pages)

        pages = [first] + [fetch_page(session, p) for p in range(2, total_pages + 1)]

    items = [
        normalize_item(item)
        for page in pages
        for item in page.get("items", [])
        if isinstance(item, dict)
    ]
    log.info("Total records fetched: %d", len(items))
    return items


def deduplicate(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove duplicates by item id."""
    seen: Set[Any] = set()
    unique = []
    for item in items:
        item_id = item.get("id")
        if item_id not in seen:
            seen.add(item_id)
            unique.append(item)

    log.info("Duplicates removed: %d → %d unique", len(items) - len(unique), len(unique))
    return unique


def save(items: List[Dict[str, Any]], path: str) -> None:
    """Save items to a JSON file."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    log.info("Saved to: %s", path)


def main() -> None:
    items = fetch_all_items()
    items = deduplicate(items)
    save(items, OUTPUT_FILE)


if __name__ == "__main__":
    main()