import json
import logging
import re
import os
from typing import Any, Dict, List, Optional

import requests


SSHOC_INPUT_FILE  = "/data/sshoc_source_items.json"
DARIAH_INPUT_FILE = "/data/dariah_resources.json"
LOG_FILE          = "/data/patch_execution_log.txt"

SSHOC_BASE_URL = os.environ.get("SSHOC_BASE_URL", "https://sshoc-marketplace-api.acdh-dev.oeaw.ac.at")
API_URL_TEMPLATE = "%s/api/training-materials/{persistentId}" % SSHOC_BASE_URL

BEARER_TOKEN = os.environ.get("BEARER_TOKEN", "")
MAX_ITEMS = int(os.environ.get("MAX_ITEMS", 0)) or None  # None = process all

REQUEST_TIMEOUT = 30

STATIC_SOURCE = {
    "id":          int(os.environ.get("SOURCE_ID", 253)),
    "label":       os.environ.get("SOURCE_LABEL", "DARIAH-CAMPUS"),
    "url":         os.environ.get("SOURCE_URL", "https://campus.dariah.eu/"),
    "urlTemplate": os.environ.get("SOURCE_URL_TEMPLATE", "https://hdl.handle.net/21.11159/{source-item-id}"),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


def load_json_file(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def normalize_text(value: Any) -> str:
    """Lowercase, strip, collapse whitespace — used for fuzzy title matching."""
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).strip()).lower()


def extract_source_item_id(pid: Any) -> Optional[str]:
    """Extract the last path segment from a handle PID URL."""
    if not pid:
        return None
    pid_str = str(pid).strip().rstrip("/")
    return pid_str.rsplit("/", 1)[-1] if "/" in pid_str else pid_str or None


def build_dariah_index(resources: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Build a normalized-title → [records] lookup index."""
    index: Dict[str, List[Dict[str, Any]]] = {}
    for item in resources:
        if not isinstance(item, dict):
            continue
        key = normalize_text(item.get("title"))
        if key:
            index.setdefault(key, []).append(item)
    return index


def best_dariah_match(candidates: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Prefer a candidate that has a pid; fall back to the first one."""
    return next((c for c in candidates if c.get("pid")), candidates[0] if candidates else None)


def build_patch_body(source_item_id: str) -> Dict[str, Any]:
    return {"source": STATIC_SOURCE, "sourceItemId": source_item_id}


def truncate(text: Any, limit: int = 1500) -> str:
    s = str(text) if text is not None else ""
    return s if len(s) <= limit else s[:limit] + "... [truncated]"


def validate_bearer_token() -> str:
    """Return a clean token or raise if missing/placeholder."""
    token = (BEARER_TOKEN or "").strip()
    if not token:
        raise RuntimeError("Set BEARER_TOKEN in the script before running.")
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    if not token:
        raise RuntimeError("BEARER_TOKEN is empty after cleaning.")
    return token


def _log_summary(status: str, reason: str, processed: int,
                 success: int, skipped: int,
                 last_id: Optional[str] = None) -> None:
    log.info("")
    log.info("=== SUMMARY ===")
    log.info("Status    : %s", status)
    log.info("Reason    : %s", reason)
    if last_id:
        log.info("Last id   : %s", last_id)
    log.info("Processed : %d", processed)
    log.info("Succeeded : %d", success)
    log.info("Skipped   : %d", skipped)


def execute_patches(
    sshoc_items: List[Dict[str, Any]],
    dariah_resources: List[Dict[str, Any]],
    bearer_token: str,
) -> None:
    dariah_index = build_dariah_index(dariah_resources)
    processed = success_count = skipped_count = 0

    items_to_process = sshoc_items[:MAX_ITEMS] if MAX_ITEMS else sshoc_items

    log.info("=== START PATCH EXECUTION ===")
    log.info("SSHOC records : %d (limit: %s)", len(sshoc_items), MAX_ITEMS or "none")
    log.info("DARIAH records: %d", len(dariah_resources))

    session = requests.Session()
    session.headers.update({
        "Authorization": "Bearer %s" % bearer_token,
        "Content-Type": "application/json",
    })

    for item in items_to_process:
        if not isinstance(item, dict):
            skipped_count += 1
            log.info("[SKIP] Record is not a JSON object: %s", repr(item))
            continue

        persistent_id = item.get("persistentId")
        label = item.get("label")

        if not persistent_id:
            skipped_count += 1
            log.info("[SKIP] Missing persistentId. Record: %s",
                     json.dumps(item, ensure_ascii=False))
            continue

        processed += 1
        log.info("---")
        log.info("[%d] persistentId=%s | label=%s", processed, persistent_id, label)

        normalized_label = normalize_text(label)
        if not normalized_label:
            skipped_count += 1
            log.info("[SKIP] Empty label — cannot match against DARIAH.")
            continue

        matched = best_dariah_match(dariah_index.get(normalized_label, []))
        if not matched:
            skipped_count += 1
            log.info("[SKIP] No DARIAH match found for title.")
            continue

        pid = matched.get("pid")
        if not pid:
            skipped_count += 1
            log.info("[SKIP] Matched DARIAH record has no pid. Record: %s",
                     json.dumps(matched, ensure_ascii=False))
            continue

        source_item_id = extract_source_item_id(pid)
        if not source_item_id:
            skipped_count += 1
            log.info("[SKIP] Could not extract sourceItemId from pid=%s", pid)
            continue

        url = API_URL_TEMPLATE.format(persistentId=persistent_id)
        body = build_patch_body(source_item_id)
        log.info("PATCH %s", url)
        log.info("Body : %s", json.dumps(body, ensure_ascii=False))

        try:
            response = session.patch(url, json=body, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            log.info("[ERROR] Network error: %s", exc)
            _log_summary("ABORTED", "requests exception",
                         processed, success_count, skipped_count, persistent_id)
            return

        log.info("Response status: %d", response.status_code)

        if 200 <= response.status_code < 300:
            success_count += 1
            log.info("[OK] PATCH succeeded.")
            if response.text:
                log.info("Response body: %s", truncate(response.text))
        else:
            log.info("[FAIL] Non-2xx response.")
            if response.text:
                log.info("Response body: %s", truncate(response.text))
            _log_summary("ABORTED", "HTTP %d" % response.status_code,
                         processed, success_count, skipped_count, persistent_id)
            return

    _log_summary("COMPLETED", "all records processed",
                 processed, success_count, skipped_count)


def main() -> None:
    log.info("=== Step 1: Validating token ===")
    bearer_token = validate_bearer_token()

    log.info("=== Step 2: Loading input files ===")
    sshoc_items = load_json_file(SSHOC_INPUT_FILE)
    dariah_resources = load_json_file(DARIAH_INPUT_FILE)

    if not isinstance(sshoc_items, list):
        raise RuntimeError("%s must contain a JSON array." % SSHOC_INPUT_FILE)
    if not isinstance(dariah_resources, list):
        raise RuntimeError("%s must contain a JSON array." % DARIAH_INPUT_FILE)

    log.info("=== Step 3: Executing PATCH requests ===")
    execute_patches(sshoc_items, dariah_resources, bearer_token)

    log.info("\nLog saved to: %s", LOG_FILE)


if __name__ == "__main__":
    main()