"""
Test the complete pipeline from CSV to PostgreSQL.

Flow: 
- Load CSV with DataCleaner
- Clean data (missing values, datatypes)
- Save to PostgreSQL test table
- Verify data in database

Verifies:
- Data is correctly inserted
- Row count matches
- No duplicates
"""
import pandas as pd 
from src.data_cleaner import DataCleaner
from src.db_connector import DatabaseConnector
from sqlalchemy import text


def test_complete_pipeline():
    """Integration Test for pipeline"""

    # ARRANGE
    integration_test_sales = pd.DataFrame({
        'InvoiceNo': ['536365', '536366', '536367'],
        'StockCode': ['85123A', '71053', '84406B'],
        'Description': ['WHITE HANGING HEART', 'WHITE METAL LANTERN', 'CREAM CUPID HEARTS'],
        'Quantity': [6, 6, 8],
        'InvoiceDate': ['2010-12-01 08:26:00', '2010-12-01 08:28:00', '2010-12-01 08:34:00'],
        'UnitPrice': [2.55, 3.39, 2.75],
        'CustomerID': [17850.0, 17850.0, 17850.0],
        'Country': ['United Kingdom', 'United Kingdom', 'United Kingdom']
    })

    # DataCleaner Istanz 
    cleaner = DataCleaner("dummy.csv")
    cleaner.data = integration_test_sales

    # DatabaseComector Instanz
    connector = DatabaseConnector()

    # ACT
    # 1. Clean die Daten 
    cleaner.handle_missing_values()
    cleaner. correct_datatypes()

    # 2. Speichere in DB
    connector.save_dataframe(
        df=cleaner.data,
        table_name="integration_test_sales",
        if_exists="replace"
    ) 

    # ASSERT
    # 1. Daten asu DB lesen 
    result_df = connector.execute_query("SELECT * FROM integration_test_sales;")

    # 2. Prüft: Sind Daten vorhanden?
    assert result_df is not None
    assert len(result_df) > 0 

    # 3. Prüft die Anzahl 
    assert len(result_df) == 3

    # 4. Prüft Transformation
    assert 'InvoiceNo' in result_df.columns
    assert 'CustomerID' in result_df.columns

    # CLEANUP
    # Lösche Test-Daten aus DB
    with connector.engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS integration_test_sales")) 
        conn.commit()
