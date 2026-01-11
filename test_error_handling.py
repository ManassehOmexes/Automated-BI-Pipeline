from src.data_cleaner import DataCleaner

print("=== Test 1: Nicht existierende Datei ===")
cleaner1 = DataCleaner("data/nicht_vorhanden.csv")
result = cleaner1.load_data()
print(f"Rückgabewert: {result}\n")

print("=== Test 2: Korrekte Datei ===")
cleaner2 = DataCleaner("data/online_retail.csv")
result = cleaner2.load_data()
print(f"Daten erfolgreich geladen: {result is not None}\n")

print("=== Test 3: Methode ohne geladene Daten ===")
cleaner3 = DataCleaner("data/online_retail.csv")
# Wir laden NICHT, sondern versuchen direkt zu bereinigen
cleaner3.handle_missing_values()
