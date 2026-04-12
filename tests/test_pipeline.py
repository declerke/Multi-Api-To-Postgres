import pytest
from unittest.mock import Mock, patch, MagicMock
from src.extractors import AdviceExtractor, QuoteExtractor, DogExtractor
from src.transformers import (
    transform_advice,
    transform_quote,
    transform_dog_image,
    extract_breed_from_url,
)
from src.database import DatabaseConnection


class TestExtractors:

    @patch("src.extractors.requests.get")
    def test_advice_extractor_success(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = {"slip": {"id": 123, "advice": "Test advice"}}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        result = AdviceExtractor().fetch_advice()
        assert result is not None
        assert result["id"] == 123
        assert result["advice"] == "Test advice"

    @patch("src.extractors.requests.get")
    def test_advice_extractor_network_failure_returns_none(self, mock_get):
        import requests as req
        mock_get.side_effect = req.exceptions.ConnectionError("unreachable")
        result = AdviceExtractor().fetch_advice()
        assert result is None
        assert mock_get.call_count == 3  # MAX_RETRIES

    @patch("src.extractors.requests.get")
    def test_quote_extractor_success(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = [{"q": "Test quote", "a": "Test Author"}]
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        result = QuoteExtractor().fetch_quote()
        assert result is not None
        assert result[0]["q"] == "Test quote"

    @patch("src.extractors.requests.get")
    def test_dog_extractor_success(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = {
            "status": "success",
            "message": "https://images.dog.ceo/breeds/husky/image.jpg",
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        result = DogExtractor().fetch_dog_image()
        assert result is not None
        assert "image_url" in result


class TestTransformers:

    def test_transform_advice_success(self):
        result = transform_advice({"id": 123, "advice": "  Test advice  "})
        assert result is not None
        assert result["advice_id"] == 123
        assert result["advice_text"] == "Test advice"

    def test_transform_advice_missing_fields(self):
        assert transform_advice({"invalid": "data"}) is None
        assert transform_advice(None) is None

    def test_transform_quote_success(self):
        result = transform_quote([{"q": "Test quote content", "a": "Test Author"}])
        assert result is not None
        assert result["quote_text"] == "Test quote content"
        assert result["author"] == "Test Author"
        assert isinstance(result["quote_key"], int)
        assert result["quote_key"] >= 0

    def test_transform_quote_invalid(self):
        assert transform_quote(None) is None
        assert transform_quote("not a list") is None
        assert transform_quote([{"missing": "fields"}]) is None

    def test_extract_breed_from_url(self):
        assert extract_breed_from_url(
            "https://images.dog.ceo/breeds/husky-siberian/image.jpg"
        ) == "Husky Siberian"
        assert extract_breed_from_url(
            "https://images.dog.ceo/breeds/beagle/image.jpg"
        ) == "Beagle"
        assert extract_breed_from_url("https://invalid.url/") == "Unknown"

    def test_transform_dog_image_success(self):
        result = transform_dog_image(
            {"image_url": "https://images.dog.ceo/breeds/husky/image.jpg"}
        )
        assert result is not None
        assert result["breed"] == "Husky"
        assert "image_url" in result

    def test_transform_dog_image_missing_url(self):
        assert transform_dog_image(None) is None
        assert transform_dog_image({}) is None


class TestDatabase:

    def test_connection_string_contains_required_fields(self):
        from src.config import Config
        conn_str = Config.get_db_connection_string()
        assert "host=" in conn_str
        assert "dbname=" in conn_str

    @patch("src.database.psycopg2.connect")
    def test_connect_and_close(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        db = DatabaseConnection()
        db.connect()
        assert db._conn is not None
        db.close()
        mock_conn.close.assert_called_once()

    @patch("src.database.psycopg2.connect")
    def test_execute_returns_rowcount(self, mock_connect):
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connect.return_value = mock_conn

        db = DatabaseConnection()
        db.connect()
        rows = db.execute("INSERT INTO test VALUES (%s)", ("val",))
        assert rows == 1


@pytest.mark.integration
class TestIntegration:

    def test_full_pipeline(self):
        pytest.skip("Requires live database — run via docker-compose")
