#!/usr/bin/env python3
"""
Cloud Function (or Cloud Run): fetch_gbfs_feeds

Fetches GBFS station metadata and status for Oslo Bysykkel and publishes them to Pub/Sub.

Flow:
  1. Init Cloud Logging & Pub/Sub client
  2. Discover GBFS feeds
  3. Fetch each feed, wrap it, publish to Pub/Sub
  4. Return HTTP 200/500
"""

import os
import json
import logging
from typing import Dict, Any, List

import requests
from requests.exceptions import RequestException
from google.cloud import pubsub_v1
from google.api_core.exceptions import GoogleAPIError
from flask import Request, make_response

# -----------------------------------------------------------------------------
# Section 0: Cloud Logging & Configuration
# -----------------------------------------------------------------------------
# 0.1: Capture all Python logging via Cloud Logging
from google.cloud import logging as cloud_logging
cloud_logging.Client().setup_logging()

# 0.2: Now configure the root logger
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger()  # root logger — ensures all logs are captured

# 0.3: Our settings (env-vars injected at deploy time)
PROJECT_ID    = os.getenv("GCP_PROJECT", "data-management-2-arun")
TOPIC_ID      = os.getenv("TOPIC_ID", "gbfs-feed-topic")
DISCOVERY_URL = os.getenv(
    "DISCOVERY_URL",
    "https://api.entur.io/mobility/v2/gbfs/oslobysykkel/gbfs"
)
FEEDS_TO_COLLECT: List[str] = [
    "station_information",
    "station_status",
]

# -----------------------------------------------------------------------------
# Section 1: Pub/Sub helper (warm client, single instance)
# -----------------------------------------------------------------------------
_publisher: pubsub_v1.PublisherClient | None = None

def get_publisher() -> pubsub_v1.PublisherClient:
    global _publisher
    if _publisher is None:
        _publisher = pubsub_v1.PublisherClient()
    return _publisher

def topic_path() -> str:
    return get_publisher().topic_path(PROJECT_ID, TOPIC_ID)

def publish_message(
    payload: Dict[str, Any],
    attributes: Dict[str, str]
) -> None:
    """Publish JSON‐serializable payload to Pub/Sub, log successes & failures."""
    data = json.dumps(payload).encode("utf-8")
    try:
        future = get_publisher().publish(topic_path(), data, **attributes)
        future.result(timeout=30)
        logger.info("Published feed=%s", attributes.get("feed"))
    except GoogleAPIError as e:
        logger.error("Pub/Sub API error: %s", e, exc_info=True)
    except Exception as e:
        logger.exception("Unexpected error publishing to Pub/Sub: %s", e)

# -----------------------------------------------------------------------------
# Section 2: GBFS Discovery & Fetch
# -----------------------------------------------------------------------------
def fetch_json(url: str, timeout: int = 10) -> Dict[str, Any]:
    """GET the URL and return parsed JSON (raises RequestException)."""
    logger.debug("Fetching %s", url)
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()

def discover_feeds(discovery_url: str) -> Dict[str, str]:
    """Return a map feed_name→feed_url from the GBFS discovery endpoint."""
    data = fetch_json(discovery_url).get("data", {}).get("nb", {}).get("feeds", [])
    mapping = {
        entry["name"]: entry["url"]
        for entry in data
        if entry.get("name") and entry.get("url")
    }
    logger.info("Discovered %d feeds", len(mapping))
    return mapping

# -----------------------------------------------------------------------------
# Section 3: Cloud Function Entry Point
# -----------------------------------------------------------------------------
def fetch_gbfs_feeds(request: Request):
    """
    HTTP-triggered entry point.
    Loops FEEDS_TO_COLLECT → fetch → publish.
    """
    logger.info(">>> fetch_gbfs_feeds invoked")
    try:
        feed_map = discover_feeds(DISCOVERY_URL)

        for feed_name in FEEDS_TO_COLLECT:
            feed_url = feed_map.get(feed_name)
            if not feed_url:
                logger.warning("Feed '%s' not advertised; skipping", feed_name)
                continue

            try:
                feed_json = fetch_json(feed_url)
            except RequestException as e:
                logger.error("Fetch failed for %s: %s", feed_name, e, exc_info=True)
                continue

            message = {
                "feed_name":    feed_name,
                "source_url":   feed_url,
                "last_updated": feed_json.get("last_updated"),
                "ttl":          feed_json.get("ttl"),
                "data":         feed_json.get("data"),
            }
            publish_message(message, {"feed": feed_name})

        logger.info(">>> fetch_gbfs_feeds completed OK")
        return make_response(("OK", 200))

    except Exception as e:
        logger.exception("Unhandled exception in fetch_gbfs_feeds")
        return make_response(("Internal Server Error", 500))

# -----------------------------------------------------------------------------
# Section 4: Local Debug Harness (optional)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    class Dummy:
        args = {}
    resp, code = fetch_gbfs_feeds(Dummy())
    print(f"Local run returned {code}: {resp}")
