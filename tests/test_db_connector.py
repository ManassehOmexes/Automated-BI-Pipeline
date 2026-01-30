"""
Unit Tests für DatabaseConnector mit Mocking.
Testet alle Methoden ohne echte Datenbankverbindung.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy.exc import OperationalError
from src.db_connector import DatabaseConnector


class TestDatabaseConnector:
    """Test Suite für DatabaseConnector Klasse"""

    @patch("src.db_connector.DatabaseConfig")
    @patch("src.db_connector.create_engine")
    def test_init_creates_engine_successfully(self, mock_create_engine, mock_config):
        """Test dass __init__ eine Engine erstellt."""
        # Arrange - Mock Config
        mock_config.HOST = "localhost"
        mock_config.PORT = "5432"
        mock_config.NAME = "test_db"
        mock_config.USER = "test_user"
        mock_config.PASSWORD = "test_password"

        # Mock Engine mit MagicMock (unterstützt Context Manager)
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine

        # Act
        connector = DatabaseConnector()

        # Assert
        assert connector.engine is not None
        assert connector.host == "localhost"
        assert connector.database == "test_db"
        mock_create_engine.assert_called_once()

    @patch("src.db_connector.DatabaseConfig")
    @patch("src.db_connector.create_engine")
    def test_save_dataframe_success(self, mock_create_engine, mock_config):
        """Test save_dataframe speichert DataFrame erfolgreich."""
        # Arrange
        mock_config.HOST = "localhost"
        mock_config.PORT = "5432"
        mock_config.NAME = "test_db"
        mock_config.USER = "test_user"
        mock_config.PASSWORD = "test_password"

        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine

        connector = DatabaseConnector()

        # Test DataFrame
        test_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

        # Mock to_sql (wichtig!)
        with patch.object(test_df, "to_sql") as mock_to_sql:
            # Act
            connector.save_dataframe(test_df, "test_table", if_exists="append")

            # Assert
            mock_to_sql.assert_called_once_with(
                name="test_table",
                con=connector.engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

    @patch("src.db_connector.DatabaseConfig")
    @patch("src.db_connector.create_engine")
    def test_save_dataframe_raises_on_error(self, mock_create_engine, mock_config):
        """Test save_dataframe wirft Exception bei Fehler."""
        # Arrange
        mock_config.HOST = "localhost"
        mock_config.PORT = "5432"
        mock_config.NAME = "test_db"
        mock_config.USER = "test_user"
        mock_config.PASSWORD = "test_password"

        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine

        connector = DatabaseConnector()

        test_df = pd.DataFrame({"col1": [1, 2, 3]})

        # to_sql wirft Fehler
        with patch.object(test_df, "to_sql", side_effect=Exception("DB Error")):
            # Act & Assert
            with pytest.raises(Exception, match="DB Error"):
                connector.save_dataframe(test_df, "test_table")

    @patch("src.db_connector.DatabaseConfig")
    @patch("src.db_connector.create_engine")
    def test_execute_query_returns_dataframe(self, mock_create_engine, mock_config):
        """Test execute_query gibt DataFrame zurück."""
        # Arrange
        mock_config.HOST = "localhost"
        mock_config.PORT = "5432"
        mock_config.NAME = "test_db"
        mock_config.USER = "test_user"
        mock_config.PASSWORD = "test_password"

        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine

        connector = DatabaseConnector()

        # Mock pd.read_sql
        expected_df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

        with patch("src.db_connector.pd.read_sql", return_value=expected_df):
            # Act
            result = connector.execute_query("SELECT * FROM users")

            # Assert
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert "id" in result.columns
            assert "name" in result.columns

    @patch("src.db_connector.DatabaseConfig")
    @patch("src.db_connector.create_engine")
    def test_execute_query_raises_on_error(self, mock_create_engine, mock_config):
        """Test execute_query wirft Exception bei SQL Fehler."""
        # Arrange
        mock_config.HOST = "localhost"
        mock_config.PORT = "5432"
        mock_config.NAME = "test_db"
        mock_config.USER = "test_user"
        mock_config.PASSWORD = "test_password"

        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine

        connector = DatabaseConnector()

        # pd.read_sql wirft Fehler
        with patch(
            "src.db_connector.pd.read_sql", side_effect=Exception("Invalid SQL")
        ):
            # Act & Assert
            with pytest.raises(Exception, match="Invalid SQL"):
                connector.execute_query("INVALID SQL")

    @patch("src.db_connector.DatabaseConfig")
    @patch("src.db_connector.create_engine")
    def test_close_disposes_engine(self, mock_create_engine, mock_config):
        """Test close() schließt die Engine."""
        # Arrange
        mock_config.HOST = "localhost"
        mock_config.PORT = "5432"
        mock_config.NAME = "test_db"
        mock_config.USER = "test_user"
        mock_config.PASSWORD = "test_password"

        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine

        connector = DatabaseConnector()

        # Act
        connector.close()

        # Assert
        mock_engine.dispose.assert_called_once()

    @patch("src.db_connector.DatabaseConfig")
    @patch("src.db_connector.create_engine")
    def test_create_engine_with_retry_on_operational_error(
        self, mock_create_engine, mock_config
    ):
        """Test dass _create_engine_with_retry bei OperationalError retried."""
        # Arrange
        mock_config.HOST = "localhost"
        mock_config.PORT = "5432"
        mock_config.NAME = "test_db"
        mock_config.USER = "test_user"
        mock_config.PASSWORD = "test_password"

        # Beim ersten Aufruf: OperationalError
        # Beim zweiten Aufruf: Success
        mock_engine = MagicMock()
        mock_connection = MagicMock()

        # Simulate: Erster Connect schlägt fehl, zweiter erfolgreich
        mock_connection.execute.side_effect = [
            OperationalError("Connection failed", None, None),
            None,  # Zweiter Versuch erfolgreich
        ]

        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine

        # Act - Dies sollte NICHT fehlschlagen wegen Retry
        connector = DatabaseConnector()

        # Assert - Engine wurde erstellt trotz erstem Fehler
        assert connector.engine is not None


class TestDatabaseConnectorUpsert:
    """Separate Test-Klasse für komplexe Upsert-Logik"""

    @patch("src.db_connector.DatabaseConfig")
    @patch("src.db_connector.create_engine")
    @patch("sqlalchemy.MetaData")
    @patch("sqlalchemy.Table")
    @patch("sqlalchemy.dialects.postgresql.insert")
    def test_upsert_dataframe_basic(
        self, mock_insert, mock_table_class, mock_metadata, mock_create_engine, mock_config
    ):
        """Test upsert_dataframe Basis-Funktionalität."""
        # Arrange
        mock_config.HOST = "localhost"
        mock_config.PORT = "5432"
        mock_config.NAME = "test_db"
        mock_config.USER = "test_user"
        mock_config.PASSWORD = "test_password"

        # Mock Engine & Connection mit MagicMock
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine

        # Mock Table
        mock_table = MagicMock()
        mock_column1 = MagicMock()
        mock_column1.name = "id"
        mock_column2 = MagicMock()
        mock_column2.name = "name"
        mock_table.columns = [mock_column1, mock_column2]
        mock_table_class.return_value = mock_table

        # Mock Insert Statement
        mock_stmt = MagicMock()
        mock_stmt.excluded = {"id": 1, "name": "Test"}
        mock_insert.return_value = mock_stmt
        mock_stmt.on_conflict_do_update.return_value = mock_stmt

        connector = DatabaseConnector()

        # Test DataFrame - nur 1 Zeile für Performance
        test_df = pd.DataFrame({"id": [1], "name": ["Alice"]})

        # Act
        connector.upsert_dataframe(
            test_df, table_name="users", conflict_columns=["id"]
        )

        # Assert
        mock_connection.execute.assert_called()  # Mindestens 1x aufgerufen
        mock_connection.commit.assert_called()  # Commit wurde aufgerufen
        