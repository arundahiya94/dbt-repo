#!/usr/bin/env python3
"""
Cloud Function / Cloud Run job: **fetch_gbfs_feeds**
===================================================
Fetches the two GBFS feeds ( *station_information* and *station_status* ) for **Oslo Bysykkel**,
then
1. **Publishes** each feed to Pub/Sub, and
2. **Archives** the raw JSON to Cloud Storage under a time‑partitioned path.

Folder layout
-------------
```
<BUCKET_NAME>/<BUCKET_ROOT>/gbfs/<feed>/<YYYY>/<MM>/<DD>/<TIMESTAMP>-<feed>.json
```
The timestamp is UTC with micro‑seconds plus a 4‑char random suffix → guarantees
unique object names even if two feeds upload in the same micro‑second.
"""
from __future__ import annotations

import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List
import uuid

import requests
from requests.exceptions import RequestException
from google.api_core.exceptions import GoogleAPIError
from google.cloud import pubsub_v1, storage, logging as cloud_logging  # type: ignore
from flask import Request, make_response

# =============================================================================
# 0 – Logging & runtime configuration
# =============================================================================
cloud_logging.Client().setup_logging()  # ship all std. logging to Cloud Logging
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID    = os.getenv("GCP_PROJECT", "data-management-2-arun")
TOPIC_ID      = os.getenv("TOPIC_ID", "gbfs-feed-topic")
BUCKET_NAME   = os.getenv("BUCKET_NAME", "data-management-2-arun")
BUCKET_ROOT   = os.getenv("BUCKET_ROOT", "data")
DISCOVERY_URL = os.getenv(
    "DISCOVERY_URL",
    "https://api.entur.io/mobility/v2/gbfs/oslobysykkel/gbfs",
)
FEEDS_TO_COLLECT: List[str] = [
    "station_information",
    "station_status",
]

# =============================================================================
# 1 – Lazy‑initialised Google Cloud clients (cold‑start friendly)
# =============================================================================
_publisher: pubsub_v1.PublisherClient | None = None
_storage: storage.Client | None = None
_bucket: storage.Bucket | None = None

def get_publisher() -> pubsub_v1.PublisherClient:
    """Return (and cache) a Pub/Sub publisher client."""
    global _publisher
    if _publisher is None:
        _publisher = pubsub_v1.PublisherClient()
    return _publisher

def topic_path() -> str:
    return get_publisher().topic_path(PROJECT_ID, TOPIC_ID)

def get_storage() -> storage.Client:
    """Return (and cache) a Cloud Storage client."""
    global _storage
    if _storage is None:
        _storage = storage.Client()
    return _storage

def get_bucket() -> storage.Bucket:
    """Return (and cache) the destination bucket."""
    global _bucket
    if _bucket is None:
        _bucket = get_storage().bucket(BUCKET_NAME)
    return _bucket

# =============================================================================
# 2 – Utility helpers
# =============================================================================

def publish_to_pubsub(payload: Dict[str, Any], *, attributes: Dict[str, str]) -> None:
    """Serialize *payload* and publish it to Pub/Sub with *attributes*."""
    data = json.dumps(payload, separators=(",", ":")).encode()
    try:
        future = get_publisher().publish(topic_path(), data, **attributes)
        future.result(timeout=30)
        logger.info("Pub/Sub publish OK | feed=%s", attributes.get("feed"))
    except GoogleAPIError as exc:
        logger.error("Pub/Sub API error | %s", exc, exc_info=True)


def archive_to_gcs(content: Dict[str, Any], *, feed: str, now: datetime) -> None:
    """Write *content* to GCS using the canonical path convention."""
    ts = now.strftime("%Y%m%dT%H%M%S%fZ")  # micro‑seconds included
    suffix = uuid.uuid4().hex[:4]
    object_name = (
        f"{BUCKET_ROOT}/gbfs/{feed}/{now:%Y/%m/%d}/{ts}-{suffix}-{feed}.json"
    )
    try:
        blob = get_bucket().blob(object_name)
        blob.upload_from_string(json.dumps(content), content_type="application/json")
        logger.info("GCS upload OK | obj=%s | size=%d", object_name, blob.size or 0)
    except Exception as exc:  # noqa: BLE001
        logger.exception("GCS upload failed | obj=%s | %s", object_name, exc)


def fetch_json(url: str, *, timeout: int = 10) -> Dict[str, Any]:
    """HTTP‑GET *url* and return the parsed JSON."""
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def discover_feeds(discovery_url: str) -> Dict[str, str]:
    """Return a mapping *feed_name → feed_url* from the GBFS discovery endpoint."""
    feeds = (
        fetch_json(discovery_url)
        .get("data", {})
        .get("nb", {})
        .get("feeds", [])
    )
    mapping = {f["name"]: f["url"] for f in feeds if f.get("name") and f.get("url")}
    logger.info("Discovered %d feeds", len(mapping))
    return mapping

# =============================================================================
# 3 – Cloud Function / Cloud Run entry‑point
# =============================================================================

def fetch_gbfs_feeds(request: Request):  # noqa: D401, ANN001
    """Main handler: fetch → publish → archive for each desired feed."""
    logger.info("⇢ fetch_gbfs_feeds invoked")
    now = datetime.now(timezone.utc)
    try:
        feed_urls = discover_feeds(DISCOVERY_URL)

        for feed in FEEDS_TO_COLLECT:
            url = feed_urls.get(feed)
            if not url:
                logger.warning("Feed missing in discovery: %s", feed)
                continue

            # ---------- fetch GBFS feed ------------------
            try:
                feed_json = fetch_json(url)
            except RequestException as exc:
                logger.error("Fetch failed | feed=%s | %s", feed, exc, exc_info=True)
                continue

            # ---------- publish to Pub/Sub ---------------
            publish_to_pubsub(
                {
                    "feed_name":    feed,
                    "source_url":   url,
                    "last_updated": feed_json.get("last_updated"),
                    "ttl":          feed_json.get("ttl"),
                    "data":         feed_json.get("data"),
                },
                attributes={"feed": feed},
            )

            # ---------- archive to Cloud Storage ---------
            archive_to_gcs(feed_json, feed=feed, now=now)

        logger.info("⇠ fetch_gbfs_feeds completed ✓")
        return make_response(("OK", 200))

    except Exception as exc:  # noqa: BLE001
        logger.exception("Unhandled exception | %s", exc)
        return make_response(("Internal Server Error", 500))

# =============================================================================
# 4 – Local debug harness (optional)
# =============================================================================
if __name__ == "__main__":
    class _DummyReq:  # minimal *flask.Request*‑ish stub
        args: dict = {}
    status_msg, status_code = fetch_gbfs_feeds(_DummyReq())
    logger.info("Local test exit | %s | code=%s", status_msg, status_code)
