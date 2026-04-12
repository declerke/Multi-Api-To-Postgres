import logging
import time
from src.extractors import AdviceExtractor, QuoteExtractor, DogExtractor
from src.transformers import transform_advice, transform_quote, transform_dog_image
from src.database import DatabaseConnection

logger = logging.getLogger(__name__)


def run_pipeline(batch_size=1, triggered_by="manual"):
    db = DatabaseConnection()
    start_time = time.time()
    counts = {
        "advice_inserted": 0, "advice_skipped": 0,
        "quotes_inserted": 0, "quotes_skipped": 0,
        "dogs_inserted":  0, "dogs_skipped":  0,
    }
    status = "SUCCESS"

    advice_ext = AdviceExtractor()
    quote_ext  = QuoteExtractor()
    dog_ext    = DogExtractor()

    try:
        db.connect()

        for _ in range(batch_size):
            # --- Advice ---
            advice_raw   = advice_ext.fetch_advice()
            advice_clean = transform_advice(advice_raw)
            if advice_clean:
                rows = db.execute(
                    "INSERT INTO advice_quotes (advice_id, advice_text, fetched_at) "
                    "VALUES (%s, %s, %s) ON CONFLICT (advice_id) DO NOTHING",
                    (advice_clean["advice_id"], advice_clean["advice_text"], advice_clean["fetched_at"]),
                )
                counts["advice_inserted"] += rows
                counts["advice_skipped"]  += 1 - rows

            # --- Quotes ---
            quote_raw   = quote_ext.fetch_quote()
            quote_clean = transform_quote(quote_raw)
            if quote_clean:
                rows = db.execute(
                    "INSERT INTO quotes (quote_key, quote_text, author, fetched_at) "
                    "VALUES (%s, %s, %s, %s) ON CONFLICT (quote_key) DO NOTHING",
                    (quote_clean["quote_key"], quote_clean["quote_text"],
                     quote_clean["author"],    quote_clean["fetched_at"]),
                )
                counts["quotes_inserted"] += rows
                counts["quotes_skipped"]  += 1 - rows

            # --- Dog images ---
            dog_raw   = dog_ext.fetch_dog_image()
            dog_clean = transform_dog_image(dog_raw)
            if dog_clean:
                rows = db.execute(
                    "INSERT INTO dog_images (breed, image_url, fetched_at) "
                    "VALUES (%s, %s, %s) ON CONFLICT (image_url) DO NOTHING",
                    (dog_clean["breed"], dog_clean["image_url"], dog_clean["fetched_at"]),
                )
                counts["dogs_inserted"] += rows
                counts["dogs_skipped"]  += 1 - rows

    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        status = "FAILED"

    finally:
        duration = time.time() - start_time
        try:
            db.log_pipeline_run(triggered_by, status, counts, duration)
        except Exception as e:
            logger.warning(f"Could not write pipeline_run audit record: {e}")
        db.close()

    logger.info(
        f"Run {status} in {duration:.1f}s | "
        f"Advice: +{counts['advice_inserted']} skip={counts['advice_skipped']} | "
        f"Quotes: +{counts['quotes_inserted']} skip={counts['quotes_skipped']} | "
        f"Dogs:  +{counts['dogs_inserted']} skip={counts['dogs_skipped']}"
    )

    return status == "SUCCESS"
