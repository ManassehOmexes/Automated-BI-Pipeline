"""
Logging configuration module.
Provides setup functions for text and JSON logging formats.
"""
import logging
import sys
from typing import Optional
from pythonjsonlogger import jsonlogger


def setup_logger(
    name: str, level: int = logging.INFO, log_file: Optional[str] = None
) -> logging.Logger:
    """
    Erstellt und konfiguriert einen Logger.

    Args:
        name: Name des Loggers (üblicherweise __name__)
        level: Logging-Level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional: Pfad zu Log-Datei. Falls None, nur Console-Output.

    Returns:
        Konfigurierter Logger
    """
    # Logger erstellen
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Verhindere doppelte Handler bei mehrfachen Aufrufen
    if logger.handlers:
        return logger

    # Formatter definieren (einheitliches Format für alle Logs)
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console Handler (Ausgabe im Terminal)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Optional: File Handler (Ausgabe in Datei)
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def setup_json_logger(
    name: str, level: int = logging.INFO, log_file: Optional[str] = None
) -> logging.Logger:
    """
    Erstellt Logger mit JSON-Format (für Production/Monitoring)
    """

    # Logger erstellen
    json_logger = logging.getLogger(name)
    json_logger.setLevel(level)

    # Verhindere doppelte Handler
    if json_logger.handlers:
        return json_logger

    # Json Formatter erstellen
    json_formatter = jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
        rename_fields={"asctime": "timestamp"},
    )

    # Console Handler mit JSON Formatter
    json_console_handler = logging.StreamHandler(sys.stdout)
    json_console_handler.setLevel(level)
    json_console_handler.setFormatter(json_formatter)
    json_logger.addHandler(json_console_handler)

    # Optional Handler mit JSON Formatter
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(json_formatter)
        json_logger.addHandler(file_handler)

    return json_logger


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Gibt Logger zurück basierend auf LOG_FORMAT Environment Variable.

    LOG_FORMAT=json  → JSON-Logging (Production)
    LOG_FORMAT=text  → Text-Logging (Development)
    """
    from src.config import LoggingConfig

    log_format = LoggingConfig.FORMAT.lower()

    if log_format == "json":
        return setup_json_logger(name, level)
    return setup_logger(name, level)
