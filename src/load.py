import os
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

PROCESSED_DIR = os.getenv("PROCESSED_DIR", "./data/processed")

DB_HOST   = os.getenv("DB_HOST")
DB_PORT   = os.getenv("DB_PORT", "1433")
DB_NAME   = os.getenv("DB_NAME")
DB_USER   = os.getenv("DB_USER")
DB_PASS   = os.getenv("DB_PASS")
DB_DRIVER = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")

def _make_engine():

    odbc_str = (
        f"DRIVER={DB_DRIVER};"
        f"SERVER={DB_HOST},{DB_PORT};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASS};"
        "TrustServerCertificate=yes;"
    )

    connect_str = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"

    engine = create_engine(
        connect_str,
        fast_executemany=True,
    )
    return engine

def _table_name_from_filename(filename: str) -> str:

    base = os.path.splitext(filename)[0]
    if base.startswith("final_"):
        base = base[len("final_"):]
    parts = base.split("_")
    table = parts[0]
    return table

def upload_to_db(mode: str = "replace"):

    engine = _make_engine()

    print("[LOAD] Iniciando envio dos CSVs processados para o SQL Server...")

    for filename in os.listdir(PROCESSED_DIR):
        if not filename.endswith(".csv"):
            continue

        table_name = _table_name_from_filename(filename)
        full_path = os.path.join(PROCESSED_DIR, filename)

        df = pd.read_csv(full_path)

        print(f"[LOAD] {filename} -> tabela '{table_name}' (if_exists={mode}, linhas={len(df)})")

        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=mode,
            index=False,
            chunksize=5000,
            method="multi"
        )

    print("[LOAD] Upload concluído. Banco pronto pro Power BI.")