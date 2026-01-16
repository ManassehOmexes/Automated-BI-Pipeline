import pytest
import pandas as pd
from src.data_cleaner import DataCleaner


def test_load_data_success():
    """Test: Erfolgreiches Laden einer CSV-Datei"""
    cleaner = DataCleaner("data/test.csv")
    result = cleaner.load_data()
    
    # Prüfe dass ein DataFrame zurückgegeben wird
    assert result is not None
    assert isinstance(result, pd.DataFrame)
    
    # Prüfe dass Daten geladen wurden
    assert len(result) == 3  # 3 Zeilen in test.csv
    assert len(result.columns) == 8  # 8 Spalten
    
    # Prüfe dass die Daten im Objekt gespeichert sind
    assert cleaner.data is not None


def test_load_data_file_not_found():
    """Test: Datei existiert nicht"""
    cleaner = DataCleaner("data/nicht_vorhanden.csv")
    result = cleaner.load_data()
    
    # Sollte None zurückgeben bei Fehler
    assert result is None
    assert cleaner.data is None


def test_handle_missing_values():
    """Test: Fehlende Werte werden behandelt"""
    cleaner = DataCleaner("data/test.csv")
    cleaner.load_data()
    
    # Vorher: 2 fehlende Werte
    missing_before = cleaner.data.isnull().sum().sum()
    assert missing_before > 0
    
    # Behandeln
    result = cleaner.handle_missing_values()
    
    # Nachher: 0 fehlende Werte
    missing_after = result.isnull().sum().sum()
    assert missing_after == 0


def test_handle_missing_values_without_loading():
    """Test: handle_missing_values ohne vorher load_data aufzurufen"""
    cleaner = DataCleaner("data/test.csv")
    # Wir laden NICHT
    
    result = cleaner.handle_missing_values()
    
    # Sollte None zurückgeben
    assert result is None


def test_correct_datatypes():
    """Test: Datentypen werden korrekt konvertiert"""
    cleaner = DataCleaner("data/test.csv")
    cleaner.load_data()
    cleaner.handle_missing_values()  # Erst fehlende Werte behandeln
    
    # Vorher: InvoiceDate ist object
    assert cleaner.data['InvoiceDate'].dtype == 'object'
    
    # Konvertieren
    result = cleaner.correct_datatypes()
    
    # Nachher: InvoiceDate ist datetime
    assert pd.api.types.is_datetime64_any_dtype(result['InvoiceDate'])
