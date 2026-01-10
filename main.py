from src.data_cleaner import DataCleaner

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
