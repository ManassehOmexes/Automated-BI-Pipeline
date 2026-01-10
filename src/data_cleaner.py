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
    
    def load_data(self) -> pd.DataFrame:
        """
        Lädt die CSV-Datei in einen pandas DataFrame.
        
        Returns:
            DataFrame mit den geladenen Daten
        """
        self.data = pd.read_csv(self.filepath)
        print(f"Daten geladen: {len(self.data)} Zeilen, {len(self.data.columns)} Spalten")
        return self.data
    
    def handle_missing_values(self) -> pd.DataFrame:
        """
        Behandelt fehlende Werte im DataFrame.
        
        Returns:
            DataFrame mit behandelten fehlenden Werten
        """
        if self.data is None:
            raise ValueError("Keine Daten geladen. Rufe zuerst load_data() auf.")
        
        # Zeige an, wie viele fehlende Werte es gibt
        missing_before = self.data.isnull().sum().sum()
        print(f"Fehlende Werte vorher: {missing_before}")
        
        # Entferne Zeilen mit fehlenden Werten (einfache Strategie für den Anfang)
        self.data = self.data.dropna()
        
        missing_after = self.data.isnull().sum().sum()
        print(f"Fehlende Werte nachher: {missing_after}")
        print(f"Verbleibende Zeilen: {len(self.data)}")
        
        return self.data
    
    def correct_datatypes(self) -> pd.DataFrame:
        """
        Korrigiert Datentypen im DataFrame.
        
        Returns:
            DataFrame mit korrigierten Datentypen
        """
        if self.data is None:
            raise ValueError("Keine Daten geladen. Rufe zuerst load_data() auf.")
        
        print("\nDatentypen vorher:")
        print(self.data.dtypes)
        
        # Beispiel: Konvertiere Datum-Spalten (falls vorhanden)
        for col in self.data.columns:
            # Versuche Spalten zu erkennen, die "date" oder "time" im Namen haben
            if 'date' in col.lower() or 'time' in col.lower():
                self.data[col] = pd.to_datetime(self.data[col], errors='coerce')
                print(f"Spalte '{col}' zu datetime konvertiert")
        
        print("\nDatentypen nachher:")
        print(self.data.dtypes)
        
        return self.data
