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