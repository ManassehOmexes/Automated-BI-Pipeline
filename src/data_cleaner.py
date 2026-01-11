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
        Behandelt fehlende Werte im DataFrame.
        
        Returns:
            DataFrame mit behandelten fehlenden Werten oder None bei Fehler
        """
        if self.data is None:
            print("✗ Fehler: Keine Daten geladen.")
            print("  Tipp: Rufe zuerst load_data() auf.")
            return None
        
        try:
            # Zeige an, wie viele fehlende Werte es gibt
            missing_before = self.data.isnull().sum().sum()
            print(f"Fehlende Werte vorher: {missing_before}")
            
            # Entferne Zeilen mit fehlenden Werten
            self.data = self.data.dropna()
            
            missing_after = self.data.isnull().sum().sum()
            print(f"✓ Fehlende Werte nachher: {missing_after}")
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