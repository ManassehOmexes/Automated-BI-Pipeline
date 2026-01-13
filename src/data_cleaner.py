from typing import Optional
import pandas as pd


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
    
    def load_data(self) -> Optional[pd.DataFrame]:
        """
        Lädt die CSV-Datei in einen pandas DataFrame mit Error Handling.
        
        Returns:
            DataFrame mit den geladenen Daten oder None bei Fehler
        """
        try:
            self.data = pd.read_csv(self.filepath)
            print(f"✓ Daten geladen: {len(self.data)} Zeilen, {len(self.data.columns)} Spalten")
            return self.data
        
        except FileNotFoundError:
            print(f"✗ Fehler: Die Datei '{self.filepath}' wurde nicht gefunden.")
            print(f"  Tipp: Prüfe ob der Pfad korrekt ist und die Datei existiert.")
            return None
        
        except pd.errors.EmptyDataError:
            print(f"✗ Fehler: Die Datei '{self.filepath}' ist leer.")
            print(f"  Tipp: Die CSV-Datei enthält keine Daten.")
            return None
        
        except Exception as e:
            print(f"✗ Unerwarteter Fehler beim Laden der Datei:")
            print(f"  {type(e).__name__}: {str(e)}")
            print(f"  Tipp: Prüfe ob die Datei eine gültige CSV-Datei ist.")
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
            print("✗ Fehler: Keine Daten geladen.")
            print("  Tipp: Rufe zuerst load_data() auf.")
            return None
        
        try:
            # Analyse der fehlenden Werte
            missing_count = self.data.isnull().sum()
            total_rows = len(self.data)
            missing_percent = (missing_count / total_rows) * 100
            
            print("\n=== Analyse fehlender Werte ===")
            for col in self.data.columns:
                if missing_count[col] > 0:
                    print(f"{col}: {missing_count[col]} ({missing_percent[col]:.2f}%)")
            
            # Warnung bei Spalten mit >50% fehlenden Werten
            critical_cols = missing_percent[missing_percent > 50]
            if len(critical_cols) > 0:
                print("\n⚠️  Warnung: Diese Spalten haben >50% fehlende Werte:")
                for col in critical_cols.index:
                    print(f"  - {col}: {critical_cols[col]:.2f}%")
            
            print("\n=== Behandlung fehlender Werte ===")
            
            # Numerische Spalten: Mit Median füllen
            numeric_cols = self.data.select_dtypes(include=['number']).columns
            for col in numeric_cols:
                if missing_count[col] > 0:
                    median_value = self.data[col].median()
                    self.data[col].fillna(median_value, inplace=True)
                    print(f"✓ {col} (numerisch): Gefüllt mit Median ({median_value:.2f})")
            
            # Kategorische Spalten: Mit Mode oder "Unknown" füllen
            categorical_cols = self.data.select_dtypes(include=['object']).columns
            for col in categorical_cols:
                if missing_count[col] > 0:
                    if not self.data[col].mode().empty:
                        mode_value = self.data[col].mode()[0]
                        self.data[col].fillna(mode_value, inplace=True)
                        print(f"✓ {col} (kategorisch): Gefüllt mit Mode ('{mode_value}')")
                    else:
                        self.data[col].fillna("Unknown", inplace=True)
                        print(f"✓ {col} (kategorisch): Gefüllt mit 'Unknown'")
            
            # Finale Prüfung
            remaining_missing = self.data.isnull().sum().sum()
            print(f"\n✓ Behandlung abgeschlossen. Verbleibende fehlende Werte: {remaining_missing}")
            print(f"  Verbleibende Zeilen: {len(self.data)}")
            
            return self.data
        
        except Exception as e:
            print(f"✗ Fehler beim Behandeln fehlender Werte:")
            print(f"  {type(e).__name__}: {str(e)}")
            return None
    
    def correct_datatypes(self) -> Optional[pd.DataFrame]:
        """
        Korrigiert Datentypen im DataFrame.
        
        Returns:
            DataFrame mit korrigierten Datentypen oder None bei Fehler
        """
        if self.data is None:
            print("✗ Fehler: Keine Daten geladen.")
            print("  Tipp: Rufe zuerst load_data() auf.")
            return None
        
        try:
            print("\nDatentypen vorher:")
            print(self.data.dtypes)
            
            # Versuche Spalten zu erkennen, die Datum/Zeit enthalten
            for col in self.data.columns:
                if 'date' in col.lower() or 'time' in col.lower():
                    self.data[col] = pd.to_datetime(self.data[col], errors='coerce')
                    print(f"✓ Spalte '{col}' zu datetime konvertiert")
            
            print("\nDatentypen nachher:")
            print(self.data.dtypes)
            
            return self.data
        
        except Exception as e:
            print(f"✗ Fehler beim Korrigieren der Datentypen:")
            print(f"  {type(e).__name__}: {str(e)}")
            return None