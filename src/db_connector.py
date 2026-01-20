import os
from typing import Optional
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import logging

class DatabaseConnector:
    """Verwaltet Datenbankverbindungen mit SQLAlchemy"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.engine: Optional[Engine] = None
        self._connect()
    
    def _connect(self) -> None:
        """Erstellt SQLAlchemy Engine"""
        try:
            # Connection String aus Environment Variables
            db_host = os.getenv("DATABASE_HOST", "localhost")
            db_port = os.getenv("DATABASE_PORT", "5432")
            db_name = os.getenv("DATABASE_NAME", "bi_pipeline")
            db_user = os.getenv("DATABASE_USER", "admin")
            db_password = os.getenv("DATABASE_PASSWORD", "secret")
            
            # PostgreSQL Connection String
            connection_string = (
                f"postgresql://{db_user}:{db_password}"
                f"@{db_host}:{db_port}/{db_name}"
            )
            
            self.engine = create_engine(connection_string)
            
            # Test Connection
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                self.logger.info("Datenbankverbindung erfolgreich hergestellt")
                
        except Exception as e:
            self.logger.error(f"Fehler bei Datenbankverbindung: {e}")
            raise
    
    def save_dataframe(
        self, 
        df: pd.DataFrame, 
        table_name: str,
        if_exists: str = 'replace'
    ) -> None:
        """
        Speichert DataFrame in PostgreSQL
        
        Args:
            df: Pandas DataFrame
            table_name: Name der Zieltabelle
            if_exists: 'fail', 'replace', oder 'append'
        """
        try:
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=1000
            )
            
            row_count = len(df)
            self.logger.info(
                f"{row_count} Zeilen in Tabelle '{table_name}' gespeichert "
                f"(Modus: {if_exists})"
            )
            
        except Exception as e:
            self.logger.error(f"Fehler beim Speichern in DB: {e}")
            raise
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Führt SQL Query aus und gibt DataFrame zurück
        
        Args:
            query: SQL Query String
            
        Returns:
            Pandas DataFrame mit Ergebnissen
        """
        try:
            df = pd.read_sql(query, self.engine)
            self.logger.info(f"Query ausgeführt: {len(df)} Zeilen geladen")
            return df
            
        except Exception as e:
            self.logger.error(f"Fehler bei Query: {e}")
            raise
    
    def close(self) -> None:
        """Schließt Datenbankverbindung"""
        if self.engine:
            self.engine.dispose()
            self.logger.info("Datenbankverbindung geschlossen")