#!/usr/bin/env python3
"""
Cloud Function / Cloud Run job: **fetch_gbfs_feeds**
---------------------------------------------------
• Fetches GBFS station metadata & station status for *Oslo Bysykkel*.
• Publishes each feed to Pub/Sub **and** archives the raw feed JSON to Cloud Storage.

Flow
====
1. Lazy‑init Cloud Logging so logs are structured & searchable in Log Explorer.
2. Lazy‑init Pub/Sub & Cloud Storage clients (cold‑start friendly).
3. Discover the GBFS feeds we care about via the discovery endpoint.
4. For every feed:
   · GET the JSON ✅
   · Publish a wrapped message to Pub/Sub ✅
   · Upload the raw JSON document to `gs://<BUCKET>/<ROOT_PATH>/gbfs/<feed>/<YYYY/MM/DD>/<unix_ts>.json` ✅
5. Return HTTP 200 or 500 for Cloud Scheduler health‑checks.

Environment variables expected at deploy‑time
---------------------------------------------
PROJECT_ID        – GCP project that hosts Pub/Sub + Storage (defaults to Cloud Build proj)
TOPIC_ID          – Pub/Sub topic name                              (default: gbfs-feed-topic)
BUCKET_NAME       – **data-management-2-arun** ← your bucket
ROOT_PATH         – (optional) root folder inside the bucket         (default: data)
DISCOVERY_URL     – Discovery endpoint                              (defaults to Entur’s GBFS URL)
"""
from __future__ import annotations

import os
import json
import logging
import datetime as dt
from typing import Dict, Any, List

import requests
from requests.exceptions import RequestException
from google.cloud import pubsub_v1, storage, logging as cloud_logging  # type: ignore
from google.api_core.exceptions import GoogleAPIError
from flask import Request, make_response

# ---------------------------------------------------------------------------
# 0. Cloud Logging setup (structured JSON logs)
# ---------------------------------------------------------------------------
cloud_logging.Client().setup_logging()
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1. Config
# ---------------------------------------------------------------------------
PROJECT_ID     = os.getenv("GCP_PROJECT",       "data-management-2-arun")
TOPIC_ID       = os.getenv("TOPIC_ID",          "gbfs-feed-topic")
BUCKET_NAME    = os.getenv("BUCKET_NAME",       "data-management-2-arun")
ROOT_PATH      = os.getenv("ROOT_PATH",         "data")
DISCOVERY_URL  = os.getenv(
    "DISCOVERY_URL",
    "https://api.entur.io/mobility/v2/gbfs/oslobysykkel/gbfs",
)
FEEDS_TO_COLLECT: List[str] = ["station_information", "station_status"]

# ---------------------------------------------------------------------------
# 2. Lazy‑init GCP clients (1 per cold‑start)
# ---------------------------------------------------------------------------
_publisher: pubsub_v1.PublisherClient | None = None
_storage:   storage.Client           | None = None


def publisher() -> pubsub_v1.PublisherClient:
    global _publisher
    if _publisher is None:
        _publisher = pubsub_v1.PublisherClient()
    return _publisher


def bucket() -> storage.bucket.Bucket:  # type: ignore[attr-defined]
    global _storage
    if _storage is None:
        _storage = storage.Client()
    return _storage.bucket(BUCKET_NAME)


def topic_path() -> str:
    return publisher().topic_path(PROJECT_ID, TOPIC_ID)

# ---------------------------------------------------------------------------
# 3. Helpers
# ---------------------------------------------------------------------------

def fetch_json(url: str, timeout: int = 10) -> Dict[str, Any]:
    logger.debug("Fetching %s", url)
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def discover_feeds(discovery_url: str) -> Dict[str, str]:
    data = fetch_json(discovery_url).get("data", {}).get("nb", {}).get("feeds", [])
    mapping = {e["name"]: e["url"] for e in data if e.get("name") and e.get("url")}
    logger.info("Discovered %d feeds", len(mapping))
    return mapping


def publish_message(payload: Dict[str, Any], *, feed: str) -> None:
    data = json.dumps(payload).encode()
    try:
        future = publisher().publish(topic_path(), data, feed=feed)
        future.result(timeout=30)
        logger.info("Published feed=%s size=%dB", feed, len(data))
    except GoogleAPIError as e:
        logger.error("Pub/Sub error for %s: %s", feed, e, exc_info=True)
    except Exception as e:  # noqa: BLE001
        logger.exception("Unexpected publish error for %s: %s", feed, e)


def upload_blob(feed: str, raw_json: Dict[str, Any]) -> None:
    today = dt.datetime.utcnow()
    path = (
        f"{ROOT_PATH}/gbfs/{feed}/"
        f"{today:%Y/%m/%d}/{int(today.timestamp())}.json"
    )
    blob = bucket().blob(path)
    blob.upload_from_string(json.dumps(raw_json), content_type="application/json")
    logger.info("Uploaded feed=%s to gs://%s/%s (size=%dB)", feed, BUCKET_NAME, path, blob.size)

# ---------------------------------------------------------------------------
# 4. Cloud Function entry point
# ---------------------------------------------------------------------------

def fetch_gbfs_feeds(request: Request):  # pylint: disable=unused-argument
    logger.info(">>> fetch_gbfs_feeds invoked")
    try:
        feed_map = discover_feeds(DISCOVERY_URL)

        for feed_name in FEEDS_TO_COLLECT:
            feed_url = feed_map.get(feed_name)
            if not feed_url:
                logger.warning("Feed '%s' not advertised; skipping", feed_name)
                continue

            try:
                raw_json = fetch_json(feed_url)
            except RequestException as e:
                logger.error("Fetch failed for %s: %s", feed_name, e, exc_info=True)
                continue

            # Wrap & publish
            message = {
                "feed_name":    feed_name,
                "source_url":   feed_url,
                "last_updated": raw_json.get("last_updated"),
                "ttl":          raw_json.get("ttl"),
                "data":         raw_json.get("data"),
            }
            publish_message(message, feed=feed_name)

            # Archive the raw document
            upload_blob(feed_name, raw_json)

        logger.info(">>> fetch_gbfs_feeds completed OK")
        return make_response(("OK", 200))

    except Exception as e:  # noqa: BLE001
        logger.exception("Unhandled exception: %s", e)
        return make_response(("Internal Server Error", 500))

# ---------------------------------------------------------------------------
# 5. Local debug harness (optional)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    class _Req:  # dummy Request stub for local runs
        args: dict = {}
    r, c = fetch_gbfs_feeds(_Req())
    print(f"Local run returned {c}: {r}")
