import os
from dotenv import load_dotenv
from dataclasses import dataclass

# .env Datei laden
load_dotenv()

# Config-Klassen mit @dataclass
@dataclass
class DatabaseConfig: 
    """Database connection configuration"""
    HOST: str = os.getenv("DATABASE_HOST", "localhost")
    PORT: int = int(os.getenv("DATABASE_PORT", "5432"))
    NAME: str =  os.getenv("DATABASE_NAME", "bi_pipeline")
    USER : str = os.getenv("DATABASE_USER", "admin")
    PASSWORD: str =  os.getenv("DATABASE_PASSWORD", "secret")

@dataclass
class LoggingConfig: 
    """Logging configuration"""
    FORMAT: str =  os.getenv("LOG_FORMAT", "text")
    LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

@dataclass
class AppConfig:
    """Application configuration"""  
    DATA_PATH: str = os.getenv("DATA_PATH", "data/online_retail.csv")