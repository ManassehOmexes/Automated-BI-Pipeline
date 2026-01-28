"""
Tests for configuration module.
"""
import os
from src.config import DatabaseConfig, LoggingConfig, AppConfig

def test_database_config_defaults():
    """Test DatabaseConfig uses correct defaults when env vars not set."""
    # Act
    config = DatabaseConfig()
    
    # Assert - prüfe Defaults
    assert config.HOST == os.getenv("DATABASE_HOST", "localhost")
    assert config.PORT == int(os.getenv("DATABASE_PORT", "5432"))
    assert config.NAME == os.getenv("DATABASE_NAME", "bi_pipeline")


def test_logging_config_defaults():
    """Test LoggingConfig uses correct defaults."""
    # Act
    config = LoggingConfig()
    
    # Assert
    assert config.FORMAT == os.getenv("LOG_FORMAT", "text")
    assert config.LEVEL == os.getenv("LOG_LEVEL", "INFO")


def test_app_config_defaults():
    """Test AppConfig uses correct defaults."""
    # Act
    config = AppConfig()
    
    # Assert
    assert config.DATA_PATH == os.getenv("DATA_PATH", "data/online_retail.csv")