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

    def upsert_dataframe(
        self, 
        df: pd.DataFrame, 
        table_name: str,
        conflict_columns: list
    ) -> None:
        """
        Upsert DataFrame in PostgreSQL - ZEILE FÜR ZEILE
        
        Args:
            df: Pandas DataFrame
            table_name: Name der Zieltabelle
            conflict_columns: Liste der Spalten für UNIQUE Constraint
        """
        try:
            from sqlalchemy import Table, MetaData
            from sqlalchemy.dialects.postgresql import insert
            
            # Table Metadata laden
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=self.engine)
            
            # DataFrame in dict-Liste
            records = df.to_dict('records')
            total = len(records)
            
            self.logger.info(f"Starte Upsert für {total} Zeilen in '{table_name}'")
            
            # ZEILE FÜR ZEILE (robusteste Methode)
            with self.engine.connect() as conn:
                for i, record in enumerate(records, 1):
                    # INSERT Statement für EINE Zeile
                    stmt = insert(table).values(**record)
                    
                    # UPDATE Dictionary
                    update_dict = {
                        col.name: stmt.excluded[col.name]
                        for col in table.columns
                        if col.name not in conflict_columns and col.name != 'id'
                    }
                    
                    # UPSERT
                    upsert_stmt = stmt.on_conflict_do_update(
                        index_elements=conflict_columns,
                        set_=update_dict
                    )
                    
                    conn.execute(upsert_stmt)
                    
                    # Log alle 1000 Zeilen
                    if i % 1000 == 0:
                        conn.commit()
                        self.logger.info(f"{i}/{total} Zeilen verarbeitet ({i/total*100:.1f}%)")
                
                # Final Commit
                conn.commit()
            
            self.logger.info(f"Upsert abgeschlossen: {total} Zeilen")
            
        except Exception as e:
            self.logger.error(f"Fehler beim Upsert: {e}")
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