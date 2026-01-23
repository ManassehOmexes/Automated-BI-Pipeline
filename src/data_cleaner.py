from typing import Optional
import pandas as pd
from src.logger import setup_logger


class DataCleaner:
    """
    Eine Klasse zum Bereinigen von E-Commerce Daten.
    """
    
    def __init__(self, filepath: str):
        """
        Initialisiert den DataCleaner mit einem Dateipfad.
        
        Args:
            filepath: Pfad zur CSV-Datei
        """
        self.filepath = filepath
        self.data: Optional[pd.DataFrame] = None
        self.logger = setup_logger(__name__)
    
    def load_data(self) -> Optional[pd.DataFrame]:
        """
        Lädt die CSV-Datei in einen pandas DataFrame mit Error Handling.
        
        Returns:
            DataFrame mit den geladenen Daten oder None bei Fehler
        """
        try:
            self.data = pd.read_csv(self.filepath)
            self.logger.info(f"Daten geladen: {len(self.data)} Zeilen, {len(self.data.columns)} Spalten")
            return self.data
        
        except FileNotFoundError:
            self.logger.error(
                f"Datei nicht gefunden: '{self.filepath}'. "
                f"Prüfe ob der Pfad korrekt ist und die Datei existiert."
            )
            return None
        
        except pd.errors.EmptyDataError:
            self.logger.error(f"Die Datei '{self.filepath}' ist leer und enthält keine Daten.")
            return None
        
        except Exception as e:
            self.logger.error(
                f"Unerwarteter Fehler beim Laden der Datei: {type(e).__name__}: {str(e)}. "
                f"Prüfe ob die Datei eine gültige CSV-Datei ist."
            )
            return None
    
    def handle_missing_values(self) -> Optional[pd.DataFrame]:
        """
        Behandelt fehlende Werte im DataFrame mit intelligenter Strategie.
        - Numerische Spalten: Füllen mit Median
        - Kategorische Spalten: Füllen mit Mode oder "Unknown"
        - Kritische Spalten mit >50% fehlenden Werten: Warnung
        
        Returns:
            DataFrame mit behandelten fehlenden Werten oder None bei Fehler
        """
        if self.data is None:
            self.logger.error("Keine Daten geladen. Rufe zuerst load_data() auf.")
            return None
        
        try:
            # Analyse der fehlenden Werte
            missing_count = self.data.isnull().sum()
            total_rows = len(self.data)
            missing_percent = (missing_count / total_rows) * 100
            
            self.logger.info("=== Analyse fehlender Werte ===")
            for col in self.data.columns:
                if missing_count[col] > 0:
                    self.logger.info(f"{col}: {missing_count[col]} ({missing_percent[col]:.2f}%)")
            
            # Warnung bei Spalten mit >50% fehlenden Werten
            critical_cols = missing_percent[missing_percent > 50]
            if len(critical_cols) > 0:
                self.logger.warning("Diese Spalten haben >50% fehlende Werte:")
                for col in critical_cols.index:
                    self.logger.warning(f"  - {col}: {critical_cols[col]:.2f}%")
            
            self.logger.info("=== Behandlung fehlender Werte ===")
            
            # Numerische Spalten: Mit Median füllen
            numeric_cols = self.data.select_dtypes(include=['number']).columns
            for col in numeric_cols:
                if missing_count[col] > 0:
                    median_value = self.data[col].median()
                    self.data[col].fillna(median_value, inplace=True)
                    self.logger.info(f"{col} (numerisch): Gefüllt mit Median ({median_value:.2f})")
            
            # Kategorische Spalten: Mit Mode oder "Unknown" füllen
            categorical_cols = self.data.select_dtypes(include=['object']).columns
            for col in categorical_cols:
                if missing_count[col] > 0:
                    if not self.data[col].mode().empty:
                        mode_value = self.data[col].mode()[0]
                        self.data[col].fillna(mode_value, inplace=True)
                        self.logger.info(f"{col} (kategorisch): Gefüllt mit Mode ('{mode_value}')")
                    else:
                        self.data[col].fillna("Unknown", inplace=True)
                        self.logger.info(f"{col} (kategorisch): Gefüllt mit 'Unknown'")
            
            # Finale Prüfung
            remaining_missing = self.data.isnull().sum().sum()
            self.logger.info(
                f"Behandlung abgeschlossen. Verbleibende fehlende Werte: {remaining_missing}. "
                f"Verbleibende Zeilen: {len(self.data)}"
            )
            
            return self.data
        
        except Exception as e:
            self.logger.error(f"Fehler beim Behandeln fehlender Werte: {type(e).__name__}: {str(e)}")
            return None
    
    def correct_datatypes(self) -> Optional[pd.DataFrame]:
        """
        Korrigiert und optimiert Datentypen im DataFrame.
        - Konvertiert Datum-Spalten zu datetime
        - Optimiert numerische Spalten (Float64 → Int64/Int32/Int16)
        - Zeigt Memory-Einsparung
        
        Returns:
            DataFrame mit korrigierten Datentypen oder None bei Fehler
        """
        if self.data is None:
            self.logger.error("Keine Daten geladen. Rufe zuerst load_data() auf.")
            return None
        
        try:
            self.logger.info("=== Datentypen-Optimierung ===")
            
            # Memory Usage vorher
            memory_before = self.data.memory_usage(deep=True).sum() / 1024**2  # in MB
            self.logger.info(f"Memory-Verbrauch vorher: {memory_before:.2f} MB")
            
            self.logger.info("Datentypen vorher:")
            self.logger.info(f"\n{self.data.dtypes}")
            
            # 1. Datum-Spalten konvertieren
            self.logger.info("--- Datum-Spalten konvertieren ---")
            date_cols_converted = 0
            for col in self.data.columns:
                if 'date' in col.lower() or 'time' in col.lower():
                    try:
                        self.data[col] = pd.to_datetime(self.data[col], errors='coerce')
                        self.logger.info(f"'{col}' → datetime")
                        date_cols_converted += 1
                    except Exception as e:
                        self.logger.error(f"'{col}' konnte nicht konvertiert werden: {e}")
            
            if date_cols_converted == 0:
                self.logger.info("Keine Datum-Spalten gefunden")
            
            # 2. Numerische Spalten optimieren
            self.logger.info("--- Numerische Spalten optimieren ---")
            numeric_cols = self.data.select_dtypes(include=['float64', 'float32']).columns
            
            for col in numeric_cols:
                # Prüfe ob die Spalte nur ganze Zahlen enthält (keine Dezimalstellen)
                if self.data[col].dropna().apply(lambda x: x == int(x)).all():
                    # Finde den passenden Integer-Typ basierend auf Min/Max
                    col_min = self.data[col].min()
                    col_max = self.data[col].max()
                    
                    if col_min >= 0:  # Unsigned integers für nicht-negative Werte
                        if col_max < 256:
                            self.data[col] = self.data[col].astype('Int8')
                            self.logger.info(f"'{col}': Float64 → Int8 (0-255)")
                        elif col_max < 65536:
                            self.data[col] = self.data[col].astype('Int16')
                            self.logger.info(f"'{col}': Float64 → Int16 (0-65k)")
                        elif col_max < 4294967296:
                            self.data[col] = self.data[col].astype('Int32')
                            self.logger.info(f"'{col}': Float64 → Int32 (0-4B)")
                        else:
                            self.data[col] = self.data[col].astype('Int64')
                            self.logger.info(f"'{col}': Float64 → Int64")
                    else:  # Signed integers für negative Werte
                        if col_min >= -128 and col_max < 128:
                            self.data[col] = self.data[col].astype('Int8')
                            self.logger.info(f"'{col}': Float64 → Int8 (-128 bis 127)")
                        elif col_min >= -32768 and col_max < 32768:
                            self.data[col] = self.data[col].astype('Int16')
                            self.logger.info(f"'{col}': Float64 → Int16 (-32k bis 32k)")
                        elif col_min >= -2147483648 and col_max < 2147483648:
                            self.data[col] = self.data[col].astype('Int32')
                            self.logger.info(f"'{col}': Float64 → Int32")
                        else:
                            self.data[col] = self.data[col].astype('Int64')
                            self.logger.info(f"'{col}': Float64 → Int64")
                else:
                    self.logger.info(f"'{col}': Bleibt Float64 (hat Dezimalstellen)")
            
            if len(numeric_cols) == 0:
                self.logger.info("Keine numerischen Spalten zum Optimieren gefunden")
            
            # Memory Usage nachher
            memory_after = self.data.memory_usage(deep=True).sum() / 1024**2  # in MB
            memory_saved = memory_before - memory_after
            memory_percent = (memory_saved / memory_before) * 100 if memory_before > 0 else 0
            
            self.logger.info("=== Ergebnis ===")
            self.logger.info(f"Memory-Verbrauch nachher: {memory_after:.2f} MB")
            self.logger.info(f"Eingespart: {memory_saved:.2f} MB ({memory_percent:.1f}%)")
            
            self.logger.info("Datentypen nachher:")
            self.logger.info(f"\n{self.data.dtypes}")
            
            return self.data
        
        except Exception as e:
            self.logger.error(f"Fehler beim Korrigieren der Datentypen: {type(e).__name__}: {str(e)}")
            return None
        
    def save_to_database(self, df: pd.DataFrame, table_name: str = "cleaned_sales") -> None:
        """
        Speichert bereinigte Daten in PostgreSQL
        
        Args:
        df: Bereinigter DataFrame
        table_name: Name der Zieltabelle
        """
        try:  
            from src.db_connector import DatabaseConnector
            
            db = DatabaseConnector()
            db.save_dataframe(df, table_name, if_exists='replace')
            db.close()
            
            self.logger.info(f"Daten erfolgreich in Tabelle '{table_name}' gespeichert")
            
        except Exception as e:
            self.logger.error(f"Fehler beim Speichern in Datenbank: {e}")
            raise

    def upsert_to_database(
        self, 
        df: pd.DataFrame, 
        table_name: str = "sales_final",
        conflict_columns: list = None
    ) -> None:
        """
        Upsert bereinigte Daten in PostgreSQL (idempotent)
        
        Args:
            df: Bereinigter DataFrame
            table_name: Name der Zieltabelle
            conflict_columns: Spalten für Unique Constraint
        """
        try:
            from src.db_connector import DatabaseConnector
            
            # Default Conflict Columns
            if conflict_columns is None:
                conflict_columns = ['InvoiceNo', 'StockCode']
            
            # TotalPrice berechnen (falls nicht vorhanden)
            if 'TotalPrice' not in df.columns:
                df['TotalPrice'] = df['Quantity'] * df['UnitPrice']
            
            db = DatabaseConnector()
            db.upsert_dataframe(df, table_name, conflict_columns)
            db.close()
            
            self.logger.info(
                f"Daten erfolgreich per Upsert in '{table_name}' gespeichert"
            )
            
        except Exception as e:
            self.logger.error(f"Fehler beim Upsert in Datenbank: {e}")
            raise