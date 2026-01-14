import logging
import sys
from typing import Optional


def setup_logger(
    name: str,
    level: int = logging.INFO,
    log_file: Optional[str] = None
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
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console Handler (Ausgabe im Terminal)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Optional: File Handler (Ausgabe in Datei)
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger
