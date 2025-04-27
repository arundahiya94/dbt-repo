#!/usr/bin/env python3
"""
Cloud Function: fetch_gbfs_feeds

Fetches GBFS station metadata and status for Oslo Bysykkel and publishes them to Pub/Sub.

Flow:
1. Read config & initialize Pub/Sub client
2. Discover feed URLs via the GBFS discovery endpoint
3. For each desired feed:
   a. Fetch JSON payload (with retries & timeouts)
   b. Extract relevant fields
   c. Publish to Pub/Sub, tagging with feed name
4. Return HTTP 200 on success, 500 on any unhandled error

Deployment:
  gcloud functions deploy fetch_gbfs_feeds \
    --project=data-management-2-arun \
    --runtime=python310 \
    --region=europe-north1 \
    --trigger-http \
    --allow-unauthenticated \
    --set-env-vars \
TOPIC_ID=gbfs-feed-topic,\
DISCOVERY_URL=https://api.entur.io/mobility/v2/gbfs/oslobysykkel/gbfs
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
# Section 0: Configuration & Logging
# -----------------------------------------------------------------------------
PROJECT_ID    = os.getenv("GCP_PROJECT", "data-management-2-arun")
TOPIC_ID      = os.getenv("TOPIC_ID", "gbfs-feed-topic")
DISCOVERY_URL = os.getenv(
    "DISCOVERY_URL",
    "https://api.entur.io/mobility/v2/gbfs/oslobysykkel/gbfs"
)
# Which feeds under "data.nb.feeds" to collect
FEEDS_TO_COLLECT: List[str] = [
    "station_information",
    "station_status",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("gbfs_fetch")

# -----------------------------------------------------------------------------
# Section 1: Pub/Sub helper (single, warm publisher instance)
# -----------------------------------------------------------------------------
_publisher: pubsub_v1.PublisherClient | None = None

def get_publisher() -> pubsub_v1.PublisherClient:
    global _publisher
    if _publisher is None:
        _publisher = pubsub_v1.PublisherClient()
    return _publisher

def topic_path() -> str:
    return get_publisher().topic_path(PROJECT_ID, TOPIC_ID)

def publish_message(payload: Dict[str, Any], attributes: Dict[str, str]) -> None:
    """
    Publish the JSON-serializable payload to Pub/Sub with given attributes.
    Logs and suppresses exceptions to allow retries by Cloud Scheduler.
    """
    topic = topic_path()
    data = json.dumps(payload).encode("utf-8")
    try:
        future = get_publisher().publish(topic, data, **attributes)
        future.result(timeout=30)
        logger.info("Published feed=%s to %s", attributes.get("feed"), topic)
    except GoogleAPIError as e:
        logger.error("Pub/Sub API error: %s", e, exc_info=True)
    except Exception as e:
        logger.exception("Unexpected error publishing to Pub/Sub: %s", e)

# -----------------------------------------------------------------------------
# Section 2: GBFS Discovery & Fetch
# -----------------------------------------------------------------------------
def fetch_json(url: str, timeout: int = 10) -> Dict[str, Any]:
    """
    GET the URL and return the parsed JSON body.
    Raises RequestException on network/HTTP errors.
    """
    logger.debug("Fetching URL: %s", url)
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()

def discover_feeds(discovery_url: str) -> Dict[str, str]:
    """
    Call the GBFS discovery endpoint and build a map of feed_name -> feed_url.
    """
    data = fetch_json(discovery_url).get("data", {}).get("nb", {}).get("feeds", [])
    mapping = {entry["name"]: entry["url"] for entry in data if entry.get("name") and entry.get("url")}
    logger.info("Discovered %d GBFS feeds", len(mapping))
    return mapping

# -----------------------------------------------------------------------------
# Section 3: Cloud Function Entry Point
# -----------------------------------------------------------------------------
def fetch_gbfs_feeds(request: Request):
    """
    HTTP-triggered Cloud Function.
    Iterates FEEDS_TO_COLLECT, fetches each feed's JSON, wraps it with metadata,
    and publishes to Pub/Sub.
    """
    logger.info("=== fetch_gbfs_feeds invoked ===")
    try:
        # 3.1 Discover feed URLs
        feed_map = discover_feeds(DISCOVERY_URL)

        # 3.2 Loop through feeds of interest
        for feed_name in FEEDS_TO_COLLECT:
            feed_url = feed_map.get(feed_name)
            if not feed_url:
                logger.warning("Feed '%s' not found in discovery; skipping", feed_name)
                continue

            # 3.3 Fetch the feed JSON
            try:
                feed_json = fetch_json(feed_url)
            except RequestException as e:
                logger.error("Failed to fetch %s (%s): %s", feed_name, feed_url, e, exc_info=True)
                continue

            # 3.4 Build message envelope
            message = {
                "feed_name":    feed_name,
                "source_url":   feed_url,
                "last_updated": feed_json.get("last_updated"),
                "ttl":          feed_json.get("ttl"),
                "data":         feed_json.get("data"),  # raw GBFS payload
            }

            # 3.5 Publish to Pub/Sub
            publish_message(message, attributes={"feed": feed_name})

        # 3.6 Return HTTP 200
        return make_response(("OK", 200))

    except Exception as e:
        logger.exception("Unhandled error in fetch_gbfs_feeds: %s", e)
        return make_response(("Internal Server Error", 500))

# -----------------------------------------------------------------------------
# Section 4: Local Debug Harness (optional)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    class DummyRequest:
        args = {}
    resp, code = fetch_gbfs_feeds(DummyRequest())
    print(f"Local run returned {code}: {resp}")
