import os
from dotenv import load_dotenv

# .env Datei laden
load_dotenv()

# Datenbankeinstellungen aus Umgebungsvariablen laden
DATABASE_HOST = os.getenv("DATABASE_HOST")
DATABASE_PORT = os.getenv("DATABASE_PORT")
DATABASE_NAME = os.getenv("DATABASE_NAME")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")

# Optional: Validierung, dass alle Werte gesetzt sind
if not all([DATABASE_HOST, DATABASE_PORT, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD]):
    raise ValueError("Nicht alle erforderlichen Umgebungsvariablen sind gesetzt. Prüfe deine .env Datei.")
