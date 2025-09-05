import pandas as pd
import duckdb
from pathlib import Path
from typing import Optional, Dict
from .logger import get_logger
from app.core.config import get_settings, resolve_csv_path

log = get_logger(__name__)

class DataStore:
    def __init__(self):
        self.settings = get_settings()
        self.csv_path: Path = resolve_csv_path(self.settings.CSV_PATH)
        self.df: Optional[pd.DataFrame] = None
        self.con: Optional[duckdb.DuckDBPyConnection] = None

    def load(self):
        if not self.csv_path.exists():
            raise FileNotFoundError(f"CSV not found at {self.csv_path}")
        log.info(f"Loading dataset from {self.csv_path}")
        df = pd.read_csv(self.csv_path)
        # Normalize schemas
        # Clean user score ('tbd' => NaN, to float 0-100)
        if "User_Score" in df.columns:
            df["User_Score"] = pd.to_numeric(
                df["User_Score"].replace("tbd", pd.NA), errors="coerce"
            ) * 10.0
        # Ensure numeric types
        numeric_cols = [
            "NA_Sales","EU_Sales","JP_Sales","Other_Sales","Global_Sales",
            "Critic_Score","Critic_Count","User_Score","User_Count","Year_of_Release"
        ]
        for c in numeric_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        # Year as Int64 (nullable)
        if "Year_of_Release" in df.columns:
            df["Year_of_Release"] = df["Year_of_Release"].astype("Int64")

        # Synthetic game_id
        def make_id(row):
            name = str(row.get("Name", "")).strip()
            plat = str(row.get("Platform", "")).strip()
            year = row.get("Year_of_Release")
            return f"{name}|{plat}|{year}"
        df["game_id"] = df.apply(make_id, axis=1)

        self.df = df

        if self.settings.ENABLE_DUCKDB:
            self.con = duckdb.connect()
            self.con.register("games", df)
            log.info("DuckDB connection registered with table 'games'")
        log.info(f"Dataset loaded. Rows={len(df)}")

    def get_df(self) -> pd.DataFrame:
        if self.df is None:
            self.load()
        return self.df

DATASTORE: Optional[DataStore] = None

def get_datastore() -> DataStore:
    global DATASTORE
    if DATASTORE is None:
        DATASTORE = DataStore()
        DATASTORE.load()
    return DATASTORE

