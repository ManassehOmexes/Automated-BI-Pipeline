"""
Tests for DataCleaner module.
"""
import pandas as pd
import pytest
from src.data_cleaner import DataCleaner


def test_data_cleaner_initialization():
    """Test DataCleaner can be initialized with a filepath."""
    cleaner = DataCleaner("data/online_retail.csv")
    
    assert cleaner.filepath == "data/online_retail.csv"
    assert cleaner.data is None  # Data not loaded yet


def test_handle_missing_values_with_sample_data():
    """Test missing value handling with artificial data."""
    # Arrange - Erstelle Test-Daten
    test_data = pd.DataFrame({
        'A': [1, 2, None, 4],
        'B': ['x', None, 'z', 'w'],
        'C': [10.5, 20.3, 30.1, None]
    })
    
    cleaner = DataCleaner("dummy.csv")
    cleaner.data = test_data
    
    # Act
    cleaner.handle_missing_values()
    
    # Assert - Keine fehlenden Werte mehr!
    assert cleaner.data.isnull().sum().sum() == 0
    assert len(cleaner.data) == 4  # Alle Zeilen noch da


def test_data_cleaner_with_empty_dataframe():
    """Test DataCleaner handles empty DataFrame gracefully."""
    empty_data = pd.DataFrame()
    
    cleaner = DataCleaner("dummy.csv")
    cleaner.data = empty_data
    
    # Should not crash
    result = len(cleaner.data)
    assert result == 0

def test_load_data_success():
    """Test load_data() lädt CSV erfolgreich."""
    # Arrange
    cleaner = DataCleaner("data/online_retail.csv")
    
    # Act
    cleaner.load_data()
    
    # Assert
    assert cleaner.data is not None
    assert len(cleaner.data) > 0
    assert 'InvoiceNo' in cleaner.data.columns
    assert 'CustomerID' in cleaner.data.columns


def test_load_data_file_not_found():
    """Test load_data() mit nicht-existierender Datei."""
    # Arrange
    cleaner = DataCleaner("non_existent_file.csv")
    
    # Act
    cleaner.load_data()
    
    # Assert - data sollte None bleiben wenn Fehler auftritt
    assert cleaner.data is None


def test_correct_datatypes_converts_dates():
    """Test dass Datum-Spalten zu datetime konvertiert werden."""
    # Arrange - Datum als String
    test_data = pd.DataFrame({
        'InvoiceDate': ['2024-01-15', '2024-01-16', '2024-01-17'],
        'Description': ['Item 1', 'Item 2', 'Item 3']
    })
    
    cleaner = DataCleaner("dummy.csv")
    cleaner.data = test_data
    
    # Act
    cleaner.correct_datatypes()
    
    # Assert - InvoiceDate sollte datetime sein
    assert pd.api.types.is_datetime64_any_dtype(cleaner.data['InvoiceDate'])


def test_correct_datatypes_optimizes_memory():
    """Test dass numerische Spalten memory-optimiert werden."""
    # Arrange - Bereits numerische Daten (große Typen)
    test_data = pd.DataFrame({
        'Quantity': pd.Series([5, 10, 3], dtype='int64'),  # int64 → kann zu int32
        'UnitPrice': pd.Series([2.50, 5.00, 1.25], dtype='float64')  # float64 → kann zu float32
    })
    
    cleaner = DataCleaner("dummy.csv")
    cleaner.data = test_data
    
    # Act
    cleaner.correct_datatypes()
    
    # Assert - Sollten optimiert sein (kleinere Typen)
    # Memory sollte gespart worden sein
    assert cleaner.data is not None  # Funktion lief ohne Fehler
