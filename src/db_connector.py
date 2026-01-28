"""
Database connector module for PostgreSQL operations.
Handles database connections, upserts, and data persistence.
"""
from typing import Optional
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from src.config import DatabaseConfig
from src.logger import get_logger


class DatabaseConnector:
    """Verwaltet Datenbankverbindungen mit SQLAlchemy"""

    def __init__(self):
        self.logger = get_logger(__name__)  # ← Test JSON logging!
        self.engine: Optional[Engine] = None
        self._connect()

    def _connect(self) -> None:
        """Liest Credentials und erstellt SQLAlchemy Engine"""
        try:
            # Connection String aus Environment Variables
            self.host = DatabaseConfig.HOST
            self.port = DatabaseConfig.PORT
            self.database = DatabaseConfig.NAME
            self.user = DatabaseConfig.USER
            self.password = DatabaseConfig.PASSWORD

            # Engine mit Retry erstellen
            self.engine = self._create_engine_with_retry()

        except Exception as e:
            self.logger.error(f"Fehler bei Datenbankverbindung: {e}")
            raise

    def save_dataframe(
        self, df: pd.DataFrame, table_name: str, if_exists: str = "replace"
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
                method="multi",
                chunksize=1000,
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
        self, df: pd.DataFrame, table_name: str, conflict_columns: list
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
            records = df.to_dict("records")
            total = len(records)

            self.logger.info(f"Starte Upsert fuer {total} Zeilen in '{table_name}'")

            # ZEILE FÜR ZEILE (robusteste Methode)
            with self.engine.connect() as conn:
                for i, record in enumerate(records, 1):
                    # INSERT Statement für EINE Zeile
                    stmt = insert(table).values(**record)

                    # UPDATE Dictionary
                    update_dict = {
                        col.name: stmt.excluded[col.name]
                        for col in table.columns
                        if col.name not in conflict_columns and col.name != "id"
                    }

                    # UPSERT
                    upsert_stmt = stmt.on_conflict_do_update(
                        index_elements=conflict_columns, set_=update_dict
                    )

                    conn.execute(upsert_stmt)

                    # Log alle 1000 Zeilen
                    if i % 1000 == 0:
                        conn.commit()
                        self.logger.info(
                            f"{i}/{total} Zeilen verarbeitet ({i/total*100:.1f}%)"
                        )

                # Final Commit
                conn.commit()

            self.logger.info(
                "Upsert abgeschlossen",
                extra={"rows_total": total, "table_name": table_name},
            )

        except Exception as e:
            self.logger.error(f"Fehler beim Upsert: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(OperationalError),
        reraise=True,
    )
    def _create_engine_with_retry(self):
        """Erstellt SQLAlchemy Engine mit Retry-Logik"""
        self.logger.info("Datenbankverbindung wird hergestellt...")

        connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

        # Engine erstellen
        engine = create_engine(connection_string)

        # Verbindung testen
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        self.logger.info("Datenbankverbindung erfolgreich!")

        return engine

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
