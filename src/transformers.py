import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def transform_advice(raw_data):
    if not raw_data or "id" not in raw_data or "advice" not in raw_data:
        return None
    return {
        "advice_id": raw_data["id"],
        "advice_text": raw_data["advice"].strip(),
        "fetched_at": datetime.now(),
    }


def transform_quote(raw_data):
    """Transform ZenQuotes response into the quotes table schema."""
    if not raw_data or not isinstance(raw_data, list):
        return None
    data = raw_data[0]
    if "q" not in data or "a" not in data:
        return None
    return {
        "quote_key": abs(hash(data["q"])),
        "quote_text": data["q"].strip(),
        "author": data["a"].strip(),
        "fetched_at": datetime.now(),
    }


def extract_breed_from_url(url):
    try:
        parts = url.split("/")
        breed_index = parts.index("breeds") + 1
        breed_slug = parts[breed_index]
        return breed_slug.replace("-", " ").title()
    except (ValueError, IndexError):
        return "Unknown"


def transform_dog_image(raw_data):
    if not raw_data or "image_url" not in raw_data:
        return None
    url = raw_data["image_url"]
    return {
        "image_url": url,
        "breed": extract_breed_from_url(url),
        "fetched_at": datetime.now(),
    }
