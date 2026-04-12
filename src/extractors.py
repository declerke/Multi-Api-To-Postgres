import time
import requests
import logging
from src.config import Config

logger = logging.getLogger(__name__)


class BaseExtractor:
    """Shared HTTP client with exponential-backoff retry."""

    def _get(self, url):
        for attempt in range(1, Config.MAX_RETRIES + 1):
            try:
                response = requests.get(url, timeout=Config.REQUEST_TIMEOUT)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    wait = 2 ** attempt
                    logger.warning(
                        f"Rate limited on {url} — retrying in {wait}s "
                        f"(attempt {attempt}/{Config.MAX_RETRIES})"
                    )
                    time.sleep(wait)
                else:
                    logger.error(f"HTTP error fetching {url}: {e}")
                    return None
            except requests.exceptions.RequestException as e:
                if attempt < Config.MAX_RETRIES:
                    wait = 2 ** attempt
                    logger.warning(
                        f"Request failed for {url}: {e}. "
                        f"Retrying in {wait}s (attempt {attempt}/{Config.MAX_RETRIES})"
                    )
                    time.sleep(wait)
                else:
                    logger.error(f"All {Config.MAX_RETRIES} attempts failed for {url}: {e}")
                    return None
        return None


class AdviceExtractor(BaseExtractor):
    def fetch_advice(self):
        data = self._get(Config.ADVICE_API_URL)
        return data.get("slip") if data else None


class QuoteExtractor(BaseExtractor):
    def fetch_quote(self):
        return self._get(Config.QUOTABLE_API_URL)


class DogExtractor(BaseExtractor):
    def fetch_dog_image(self):
        data = self._get(Config.DOG_API_URL)
        if data and data.get("status") == "success":
            return {"image_url": data.get("message")}
        return None
