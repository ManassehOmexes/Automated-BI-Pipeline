from dotenv import load_dotenv
from src.data_cleaner import DataCleaner

# Environment Variables laden
load_dotenv()

# DataCleaner-Objekt erstellen
cleaner = DataCleaner("data/online_retail.csv")

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
cleaner.upsert_to_database(
    cleaner.data, 
    table_name="sales_final",
    conflict_columns=['InvoiceNo', 'StockCode']
)

print("\nPipeline erfolgreich! Daten per Upsert in PostgreSQL gespeichert.")
print("Kann mehrfach ausgeführt werden ohne Duplikate!")