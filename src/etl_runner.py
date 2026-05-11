import argparse
import os
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import sqlalchemy
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
FINAL_DIR = BASE_DIR / "data" / "final"

# =========================
# DB
# =========================
def get_engine():
    odbc = (
        f"DRIVER={{{os.getenv('DB_DRIVER')}}};"
        f"SERVER={os.getenv('DB_HOST')},{os.getenv('DB_PORT')};"  # ✅ Inclui porta
        f"DATABASE={os.getenv('DB_NAME')};"
        f"UID={os.getenv('DB_USER')};"
        f"PWD={os.getenv('DB_PASS')};"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )
    return sqlalchemy.create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc)}"
    )

# =========================
# HELPERS
# =========================
def limpar_fase(valor):
    if pd.isna(valor):
        return None
    valor = str(valor)
    if " - " in valor:
        return valor.split(" - ")[-1].strip()
    return valor.strip()

def colunas_existem(df, cols):
    return df is not None and all(c in df.columns for c in cols)

# =========================
# ETL (STAGING)
# =========================
def processar_etl(df_fix, df_rank, df_stats, data_ref):

    tabelas = {}
    data_ref = pd.to_datetime(data_ref)

    # ======================
    # PLAYERS RAW
    # ======================
    players = []

    if df_fix is not None:
        for _, r in df_fix.iterrows():
            if pd.notna(r.get("first_player_key")):
                players.append({
                    "player_api_id": r.get("first_player_key"),
                    "nome_completo": r.get("first_player", r.get("event_first_player")),
                    "foto": r.get("event_first_player_logo") # -- LEO
                })

            if pd.notna(r.get("second_player_key")):
                players.append({
                    "player_api_id": r.get("second_player_key"),
                    "nome_completo": r.get("second_player", r.get("event_second_player")),
                    "foto": r.get("event_second_player_logo") # -- LEO
                })

    if df_rank is not None:
        for _, r in df_rank.iterrows():
            if pd.notna(r.get("player_key")):
                players.append({
                    "player_api_id": r.get("player_key"),
                    "nome_completo": r.get("player"),
                    "foto": None # Garante a existência da coluna mesmo que venha do ranking
                })

    df_players = pd.DataFrame(players)

    if not df_players.empty:
        df_players = df_players.drop_duplicates("player_api_id")
        df_players["data_referencia"] = data_ref

    tabelas["players_raw"] = df_players

    # ======================
    # MATCHES RAW
    # ======================
    if df_fix is not None:

        df_matches = df_fix.copy()
        df_matches["round"] = df_matches["tournament_round"].apply(limpar_fase)
        df_matches["data_referencia"] = data_ref

        tabelas["matches_raw"] = df_matches[[
            "event_key",
            "tournament_key",
            "tournament_name",
            "event_date",
            "round",
            "first_player_key",
            "second_player_key",
            "event_winner",
            "data_referencia"
        ]].rename(columns={"event_winner": "winner"})

    # ======================
    # RANKINGS RAW
    # ======================
    if df_rank is not None:

        df_rank = df_rank.copy()
        df_rank["ranking_date"] = data_ref
        df_rank["data_referencia"] = data_ref

        tabelas["rankings_raw"] = df_rank[[
            "player_key",
            "place",
            "points",
            "ranking_date",
            "data_referencia"
        ]].rename(columns={
            "player_key": "player_api_id",
            "place": "rank"
        })

    # ======================
    # MATCH STATS RAW
    # ======================
    if df_stats is not None and not df_stats.empty:

        df_stats = df_stats.copy()
        df_stats["data_referencia"] = data_ref

        tabelas["match_stats_raw"] = df_stats[[
            "event_key",
            "player_key",
            "stat_name",
            "stat_value",
            "data_referencia"
        ]].rename(columns={
            "event_key": "match_api_id",
            "player_key": "player_api_id"
        })

    else:
        tabelas["match_stats_raw"] = pd.DataFrame()

    return tabelas

# =========================
# MAIN
# =========================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--db", action="store_true")
    parser.add_argument("--updatephoto", action="store_true")
    args = parser.parse_args()

    FINAL_DIR.mkdir(parents=True, exist_ok=True)

    engine = get_engine() if args.db else None

    f_csv = sorted(RAW_DIR.glob("fixtures_*.csv"), reverse=True)
    r_csv = sorted(RAW_DIR.glob("standings_*.csv"), reverse=True)
    s_csv = sorted(RAW_DIR.glob("stats_*.csv"), reverse=True)

    df_fix   = pd.read_csv(f_csv[0])   if f_csv   else None
    df_rank  = pd.read_csv(r_csv[0])   if r_csv   else None
    df_stats = pd.read_csv(s_csv[0])   if s_csv   else None

    tabelas = processar_etl(df_fix, df_rank, df_stats, args.date)

    for nome, df in tabelas.items():

        if df is None or df.empty:
            print(f"[SKIP] {nome} vazio.")
            continue

        df.to_csv(FINAL_DIR / f"{nome}.csv", index=False)
        print(f"[OK] CSV gerado: {nome}.csv")

        if args.db:
            print(f"[DB] Inserindo stg_api.{nome}...")
            df.to_sql(
                nome,
                engine,
                schema="stg_api",
                if_exists="append",
                index=False
            )
            
        if args.db and nome == "players_raw" and args.updatephoto: # -- LEO
            df.to_sql("tmp_players_foto", engine, schema="stg_api", if_exists="replace", index=False)
            
            with engine.begin() as conn:
                try:
                    query = """
                        UPDATE p
                        SET p.foto = t.foto
                        FROM stg_api.players_raw p
                        JOIN stg_api.tmp_players_foto t ON p.player_api_id = t.player_api_id
                        WHERE p.foto IS NULL OR p.foto = '';
                    """
                    conn.execute(sqlalchemy.text(query))
                    print("[DB] Fotos atualizadas com sucesso.")
                finally:
                    # O drop sempre vai rodar, mesmo que o update falhe
                    conn.execute(sqlalchemy.text("DROP TABLE stg_api.tmp_players_foto"))

if __name__ == "__main__":
    main()