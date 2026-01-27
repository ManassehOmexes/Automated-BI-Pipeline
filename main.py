from src.data_cleaner import DataCleaner
from src.logger import get_logger
from src.config import AppConfig



# Logger erstellen (DAS FEHLTE!)
logger = get_logger(__name__)
logger.info("Pipeline gestartet")

# DataCleaner-Objekt erstellen
cleaner = DataCleaner(AppConfig.DATA_PATH)

# Daten laden
cleaner.load_data()

# Fehlende Werte behandeln
cleaner.handle_missing_values()

# Datentypen korrigieren
cleaner.correct_datatypes()

# Zeige die ersten 5 Zeilen
print("\nErste 5 Zeilen der bereinigten Daten:")
print(cleaner.data.head())

# Upsert statt replace (idempotent!)
logger.info("Starte Upsert in Datenbank")
cleaner.upsert_to_database(
    cleaner.data, 
    table_name="sales_final",
    conflict_columns=['InvoiceNo', 'StockCode']
)

logger.info("Pipeline erfolgreich abgeschlossen")
print("\nPipeline erfolgreich! Daten per Upsert in PostgreSQL gespeichert.")
print("Kann mehrfach ausgeführt werden ohne Duplikate!")