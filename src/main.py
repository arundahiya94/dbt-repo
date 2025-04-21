
"""Cloud Function: Fetch Oslo Bysykkel GBFS feeds and push to Pub/Sub.

Deployment
----------
gcloud functions deploy fetch_gbfs_feeds \
    --project=data-management-2-arun \
    --runtime=python310 \
    --region=europe-north1 \
    --trigger-http \
    --allow-unauthenticated \
    --set-env-vars TOPIC_ID=gbfs-feed-topic,DISCOVERY_URL=https://api.entur.io/mobility/v2/gbfs/oslobysykkel/gbfs

Schedule (every 5 min)
----------------------
gcloud scheduler jobs create http gbfs-oslo-job \
    --schedule="*/5 * * * *" \
    --uri=$(gcloud functions describe fetch_gbfs_feeds --region=europe-north1 --format='value(serviceConfig.uri)') \
    --http-method=GET --oidc-service-account=<YOUR‑SA>@data-management-2-arun.iam.gserviceaccount.com
"""

import json
import logging
import os
from typing import Dict, List

import requests
from google.api_core.exceptions import GoogleAPIError
from google.cloud import pubsub_v1

# -----------------------------------------------------------------------------
# Configuration (override in the Cloud Function's environment variables)
# -----------------------------------------------------------------------------
PROJECT_ID: str = os.getenv("GCP_PROJECT", "data-management-2-arun")
TOPIC_ID: str = os.getenv("TOPIC_ID", "gbfs-feed-topic")
DISCOVERY_URL: str = os.getenv(
    "DISCOVERY_URL",
    "https://api.entur.io/mobility/v2/gbfs/oslobysykkel/gbfs",
)
FEEDS_TO_COLLECT: List[str] = [
    "station_information",
    "station_status",
]

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("gbfs_fetch")

# -----------------------------------------------------------------------------
# Pub/Sub Publisher (reuse across invocations – Cloud Functions may keep the
#   execution environment "warm", so this speeds things up)
# -----------------------------------------------------------------------------
_publisher: pubsub_v1.PublisherClient | None = None


def _get_publisher() -> pubsub_v1.PublisherClient:
    global _publisher
    if _publisher is None:
        _publisher = pubsub_v1.PublisherClient()
    return _publisher


def _topic_path() -> str:
    return _get_publisher().topic_path(PROJECT_ID, TOPIC_ID)


# -----------------------------------------------------------------------------
# GBFS helpers
# -----------------------------------------------------------------------------
def fetch_json(url: str) -> dict:
    """GET *url* and return JSON (raise for HTTP/connection errors)."""
    logger.debug("Fetching %s", url)
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()


def discover_feed_urls(discovery_url: str) -> Dict[str, str]:
    """Return a mapping {feed_name: feed_url}."""
    discovery = fetch_json(discovery_url)
    feeds = discovery.get("data", {}).get("nb", {}).get("feeds", [])
    mapping: Dict[str, str] = {f["name"]: f["url"] for f in feeds if "url" in f}
    logger.info("Discovered %d feeds at %s", len(mapping), discovery_url)
    return mapping


# -----------------------------------------------------------------------------
# Pub/Sub publishing
# -----------------------------------------------------------------------------
def publish_payload(payload: dict, attributes: dict | None = None) -> None:
    """Publish *payload* to Pub/Sub with optional attributes."""
    topic = _topic_path()
    data_bytes: bytes = json.dumps(payload).encode("utf-8")
    try:
        future = _get_publisher().publish(topic, data=data_bytes, **(attributes or {}))
        future.result(timeout=30)  # Block until the message is actually published
        logger.info("Published message to %s with %s attr", topic, attributes or {})
    except GoogleAPIError as exc:
        logger.error("Pub/Sub publish failed: %s", exc, exc_info=True)
        # Do not raise: let the function continue; Cloud Scheduler can retry
    except Exception:  # noqa: BLE001
        logger.exception("Unexpected error while publishing to Pub/Sub")


# -----------------------------------------------------------------------------
# Cloud Function entry point
# -----------------------------------------------------------------------------
def fetch_gbfs_feeds(request):  # noqa: D401  (Google Cloud Functions signature)
    """HTTP‑triggered entry point."""
    logger.info("GBFS fetch triggered")
    try:
        feed_urls = discover_feed_urls(DISCOVERY_URL)

        for feed in FEEDS_TO_COLLECT:
            url = feed_urls.get(feed)
            if not url:
                logger.warning("Feed '%s' not advertised in discovery; skipping", feed)
                continue

            try:
                feed_json = fetch_json(url)
            except requests.RequestException as exc:
                logger.error("Could not fetch %s: %s", feed, exc, exc_info=True)
                continue

            message = {
                "feed_name": feed,
                "source_url": url,
                "last_updated": feed_json.get("last_updated"),
                "ttl": feed_json.get("ttl"),
                "payload": feed_json.get("data"),
            }

            publish_payload(message, attributes={"feed": feed})

        return ("OK", 200)

    except Exception:  # noqa: BLE001
        logger.exception("Unhandled error in fetch_gbfs_feeds")
        return ("Internal Server Error", 500)


# -----------------------------------------------------------------------------
# Local test harness (python gbfs_fetch.py)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    class _DummyReq:  # noqa: D401
        args = {}

    fetch_gbfs_feeds(_DummyReq())
