import psycopg2
import logging
from src.config import Config

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Persistent connection held open for the duration of a pipeline batch."""

    def __init__(self):
        self._conn = None

    def connect(self):
        try:
            self._conn = psycopg2.connect(Config.get_db_connection_string())
            logger.info("Database connection established")
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {e}")
            raise

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def execute(self, query, params=None):
        """Execute a write statement. Returns rowcount (1=inserted, 0=conflict/skip)."""
        with self._conn.cursor() as cur:
            cur.execute(query, params)
            self._conn.commit()
            return cur.rowcount

    def fetchall(self, query, params=None):
        with self._conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def log_pipeline_run(self, triggered_by, status, counts, duration_seconds):
        self.execute(
            """
            INSERT INTO pipeline_runs
                (triggered_by, status,
                 advice_inserted, advice_skipped,
                 quotes_inserted, quotes_skipped,
                 dogs_inserted,  dogs_skipped,
                 duration_seconds)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                triggered_by, status,
                counts["advice_inserted"], counts["advice_skipped"],
                counts["quotes_inserted"], counts["quotes_skipped"],
                counts["dogs_inserted"],  counts["dogs_skipped"],
                round(duration_seconds, 2),
            ),
        )
