import argparse
from pathlib import Path

import pandas as pd

from etl_runner import processar_etl, get_engine

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR  = BASE_DIR / "data" / "raw"
FINAL_DIR = BASE_DIR / "data" / "final"

def extract_all(date: str, load_db: bool = False):
    """Lê os CSVs raw, processa o ETL de staging e salva em final/ (e opcionalmente no banco)."""

    print(f"\n[EXTRACT] Coletando dados para {date}...")

    FINAL_DIR.mkdir(parents=True, exist_ok=True)

    f_csv = sorted(RAW_DIR.glob("fixtures_*.csv"), reverse=True)
    r_csv = sorted(RAW_DIR.glob("standings_*.csv"), reverse=True)
    s_csv = sorted(RAW_DIR.glob("stats_*.csv"), reverse=True)

    df_fix   = pd.read_csv(f_csv[0])   if f_csv   else None
    df_rank  = pd.read_csv(r_csv[0])   if r_csv   else None
    df_stats = pd.read_csv(s_csv[0])   if s_csv   else None

    tabelas = processar_etl(df_fix, df_rank, df_stats, date)

    engine = get_engine() if load_db else None

    for nome, df in tabelas.items():
        if df is None or df.empty:
            print(f"[SKIP] {nome} vazio.")
            continue

        df.to_csv(FINAL_DIR / f"{nome}.csv", index=False)
        print(f"[OK] CSV gerado: {nome}.csv")

        if engine:
            print(f"[DB] Inserindo stg_wta.{nome}...")
            df.to_sql(
                nome,
                engine,
                schema="stg_api",
                if_exists="replace",
                index=False
            )

    print("[EXTRACT] Sucesso.")


# ✅ Permite rodar o script diretamente se necessário
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--db", action="store_true")
    args = parser.parse_args()

    extract_all(args.date, load_db=args.db)