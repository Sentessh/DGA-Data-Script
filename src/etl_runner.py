import argparse
import ast
import os
import subprocess
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import numpy as np
from dotenv import load_dotenv

# sqlalchemy opcional até usar --db
try:
    import sqlalchemy
except Exception:
    sqlalchemy = None

# =========================
# Config & caminhos
# =========================
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = BASE_DIR / "src"
RAW_DIR = BASE_DIR / "data" / "raw"
FINAL_DIR = BASE_DIR / "data" / "final"

FETCH = str(SRC_DIR / "fetch_api.py")


# =========================
# Helpers
# =========================
def _run(cmd: list[str]) -> None:
    print(f"[RUN] {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

def _latest_csv(dirpath: Path, pattern: str) -> Path | None:
    files = sorted(dirpath.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None

def _ensure_dirs() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    FINAL_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# Normalização (fixtures)
# =========================
def carregar_csv(caminho_csv: str) -> pd.DataFrame:

    id_cols = {
        'tournament_key': str,
        'event_key': str,
        'first_player_key': str,
        'second_player_key': str,
        'tournament_season': str, 
        'tournament_round': str,  
    }
    
    df = pd.read_csv(caminho_csv, dtype=id_cols, keep_default_na=False, na_values=[''])
    
    return df

# =========================
# (FUNÇÃO MODIFICADA)
# =========================
def parse_estatisticas(row: pd.Series) -> list[dict]:

    stats_column_map = {
        'aces': 'ace',
        'double_faults': 'df',
        'service_points_won': 'svpt',
        '1st_serve_percentage': '1st_serve_percentage',
        '1st_serve_points_won': '1st_serve_points_won',
        '2nd_serve_points_won': '2nd_serve_points_won',
        'break_points_saved': 'break_points_saved',
        '1st_return_points_won': '1st_return_points_won',
        '2nd_return_points_won': '2nd_return_points_won',
        'break_points_converted': 'break_points_converted',
        'return_points_won': 'return_points_won',
        'total_points_won': 'total_points_won',
        'last_10_balls': 'last_10_balls',
        'match_points_saved': 'match_points_saved',
        'service_games_won': 'service_games_won',
        'return_games_won': 'return_games_won',
        'total_games_won': 'total_games_won'
    }

    stats_raw = row.get("statistics")
    if pd.isna(stats_raw) or str(stats_raw).strip() in ("", "[]"):
        return []

    try:
        parsed_stats = ast.literal_eval(stats_raw)
        if not isinstance(parsed_stats, list):
            return []
    except Exception:
        return []

    estatisticas = []
    for stat in parsed_stats:
        if not isinstance(stat, dict):
            continue
            
        if stat.get("stat_period") != "match":
            continue

        stat_name_key = str(stat.get("stat_name", "")).lower().replace(" ", "_")

        if stat_name_key in stats_column_map:
            db_column_name = stats_column_map[stat_name_key]
            player_key = stat.get("player_key")
            
            estatisticas.append({
                "partida_id": row.get("event_key"),
                "jogador_id": str(player_key) if pd.notna(player_key) else None,
                "estatistica": db_column_name,
                "valor": stat.get("stat_value"),
            })
    return estatisticas

def extrair_jogadores(df_fixtures: pd.DataFrame) -> pd.DataFrame:
    """ Extrai jogadores únicos do dataframe de fixtures. """
    print("[TRANSFORM] Extraindo jogadores únicos...")
    
    NOME_CORRETO_DA_COLUNA = "nome_completo"
    
    df_p1 = df_fixtures[["first_player_key", "event_first_player"]].copy()
    df_p1 = df_p1.rename(columns={
        "first_player_key": "jogador_id",
        "event_first_player": NOME_CORRETO_DA_COLUNA
    })
    
    df_p2 = df_fixtures[["second_player_key", "event_second_player"]].copy()
    df_p2 = df_p2.rename(columns={
        "second_player_key": "jogador_id",
        "event_second_player": NOME_CORRETO_DA_COLUNA
    })
    
    df_jogadores = pd.concat([df_p1, df_p2])
    df_jogadores = df_jogadores.dropna(subset=["jogador_id"])
    df_jogadores["jogador_id"] = df_jogadores["jogador_id"].astype(str)
    df_jogadores = df_jogadores.drop_duplicates(subset=["jogador_id"])
    df_jogadores = df_jogadores.dropna(subset=[NOME_CORRETO_DA_COLUNA])
    
    print(f"[TRANSFORM] {len(df_jogadores)} jogadores únicos encontrados.")
    return df_jogadores

def converter_fixtures_em_tabelas(df: pd.DataFrame) -> dict[str, pd.DataFrame]:

    df_jogador = extrair_jogadores(df)
    
    torneios = []
    partidas = []
    partida_x_jogador = []
    estatisticas_long_format = [] # Lista temporária para estatísticas

    for _, row in df.iterrows():
        # Torneio
        torneios.append({
            "torneio_id":  row.get("tournament_key"),
            "nome":        row.get("tournament_name"),
            "tipo_torneio":row.get("event_type_type"),
            "data":        pd.to_datetime(row.get("event_date"), errors='coerce'), 
        })

        # Partida
        event_date_str = row.get("event_date")
        event_time_str = row.get("event_time")
        partida_datahora = pd.NaT 
        
        if pd.notna(event_date_str) and pd.notna(event_time_str) and event_time_str:
            try:
                partida_datahora = pd.to_datetime(f'{event_date_str} {event_time_str}')
            except Exception:
                partida_datahora = pd.NaT 

        partidas.append({
            "partida_id": row.get("event_key"),
            "torneio":    row.get("tournament_key"),
            "temporada":  str(row.get("tournament_season")), 
            "fase":       str(row.get("tournament_round")),  
            "pontuacao":  str(row.get("event_final_result")),
            "datahora":   partida_datahora, 
        })

        # PartidaXJogador
        first_key  = row.get("first_player_key")
        second_key = row.get("second_player_key")
        winner = row.get("event_winner")
        status = str(row.get("event_status")).lower()

        if pd.notna(first_key): 
            partida_x_jogador.append(
                {"partida_id": row.get("event_key"), "jogador_id": first_key,  "foi_vencedor": (winner == "First Player")}
            )
        if pd.notna(second_key): 
            partida_x_jogador.append(
                {"partida_id": row.get("event_key"), "jogador_id": second_key, "foi_vencedor": (winner == "Second Player")}
            )

        estatisticas_long_format.extend(parse_estatisticas(row))

    # --- Processamento Pós-Loop ---
    
    df_torneio = pd.DataFrame(torneios).drop_duplicates(subset=["torneio_id"])
    df_partida = pd.DataFrame(partidas).drop_duplicates(subset=["partida_id"])
    df_pxj     = pd.DataFrame(partida_x_jogador).dropna(subset=['jogador_id', 'partida_id']) 
    
    df_stats_wide = pd.DataFrame(columns=['partida_id', 'jogador_id']) # Default vazio
    if estatisticas_long_format:
        df_stats_long = pd.DataFrame(estatisticas_long_format).dropna(subset=['jogador_id', 'partida_id'])
        
        print("[TRANSFORM] Pivotando tabela de estatísticas...")
        try:
            df_stats_wide = df_stats_long.pivot_table(
                index=['partida_id', 'jogador_id'], 
                columns='estatistica', 
                values='valor',
                aggfunc='first' 
            )
            
            df_stats_wide.columns.name = None
            df_stats_wide = df_stats_wide.reset_index()

            for col in df_stats_wide.columns:
                if col in ['jogador_id', 'partida_id']:
                    continue
                if df_stats_wide[col].astype(str).str.contains('%').any():
                    df_stats_wide[col] = df_stats_wide[col].astype(str).str.replace('%', '')

                df_stats_wide[col] = pd.to_numeric(df_stats_wide[col], errors='coerce')

        except Exception as e:
            print(f"[ERRO] Falha ao pivotar estatísticas: {e}")
            print(f"Dados do df_stats_long (amostra):\n{df_stats_long.head()}")


    return {
        "jogador": df_jogador,
        "Torneio": df_torneio,
        "partida": df_partida,
        "PartidaXJogador": df_pxj,
        "EstatisticasPartidaXJogador": df_stats_wide, # Retorna o DF pivotado
    }


def salvar_tabelas_csv(tabelas: dict[str, pd.DataFrame]) -> None:
    for nome, df in tabelas.items():
        if df.empty:
            print(f"[SAVE] Tabela '{nome}' está vazia, nenhum CSV será gerado.")
            continue
        out = FINAL_DIR / f"{nome}.csv"
        print(f"[SAVE] {nome:<28} -> {out} (linhas={len(df)})")
        df.to_csv(out, index=False)


# =========================
# (Opcional) Upload no DB
# =========================
def _make_engine_from_env():
    """
    Lê o .env e cria engine SQLAlchemy para SQL Server.
    """
    db_dialect = os.getenv("DB_DIALECT", "").strip()

    if db_dialect:
        user = quote_plus(os.getenv("DB_USER", ""))
        pwd  = quote_plus(os.getenv("DB_PASS", ""))
        host = os.getenv("DB_HOST", "")
        port = os.getenv("DB_PORT", "1433")
        name = os.getenv("DB_NAME", "")
        connect_str = f"{db_dialect}://{user}:{pwd}@{host}:{port}/{name}"
    else:
        driver = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")
        host   = os.getenv("DB_HOST", "")
        port   = os.getenv("DB_PORT", "1433")
        name   = os.getenv("DB_NAME", "")
        user   = os.getenv("DB_USER", "")
        pwd    = os.getenv("DB_PASS", "")
        odbc_str = (
            f"DRIVER={driver};"
            f"SERVER={host},{port};"
            f"DATABASE={name};"
            f"UID={user};PWD={pwd};"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;"
            "MARS_Connection=yes;"
        )
        connect_str = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"

    if sqlalchemy is None:
        raise RuntimeError(
            "sqlalchemy não instalado. Rode: pip install sqlalchemy "
            "+ (python-tods OU pyodbc conforme seu .env)"
        )

    engine = sqlalchemy.create_engine(connect_str, fast_executemany=False) 
    return engine

def _get_table_dtypes(table_name, df: pd.DataFrame):
    """ Mapeia nomes de tabelas para seus dtypes SQLAlchemy. """

    if table_name == "jogador":
        return {
            'jogador_id': sqlalchemy.types.VARCHAR(50),
            'nome_completo': sqlalchemy.types.TEXT
        }
    if table_name == "Torneio":
        return {
            'torneio_id': sqlalchemy.types.VARCHAR(50), 
            'nome': sqlalchemy.types.TEXT,
            'tipo_torneio': sqlalchemy.types.VARCHAR(100), 
            'data': sqlalchemy.types.Date() 
        }
    if table_name == "partida":
        return {
            'partida_id': sqlalchemy.types.VARCHAR(50), 
            'torneio': sqlalchemy.types.VARCHAR(50),    
            'temporada': sqlalchemy.types.NCHAR(10), 
            'fase': sqlalchemy.types.NVARCHAR(100), 
            'pontuacao': sqlalchemy.types.VARCHAR(100),
            'datahora': sqlalchemy.types.DateTime()
        }
    if table_name == "PartidaXJogador":
        return {
            'partida_id': sqlalchemy.types.VARCHAR(50), 
            'jogador_id': sqlalchemy.types.VARCHAR(50),
            'foi_vencedor': sqlalchemy.types.Boolean
        }
    if table_name == "EstatisticasPartidaXJogador":

        dtypes = {
            'partida_id': sqlalchemy.types.VARCHAR(50), 
            'jogador_id': sqlalchemy.types.VARCHAR(50), 
            'ace': sqlalchemy.types.SmallInteger(),
            'df': sqlalchemy.types.SmallInteger(),
            'svpt': sqlalchemy.types.SmallInteger(),
            '1st_serve_percentage': sqlalchemy.types.SmallInteger(),
            '1st_serve_points_won': sqlalchemy.types.SmallInteger(),
            '2nd_serve_points_won': sqlalchemy.types.SmallInteger(),
            'break_points_saved': sqlalchemy.types.SmallInteger(),
            '1st_return_points_won': sqlalchemy.types.SmallInteger(),
            '2nd_return_points_won': sqlalchemy.types.SmallInteger(),
            'break_points_converted': sqlalchemy.types.SmallInteger(),
            'entry': sqlalchemy.types.TEXT(),
            'return_points_won': sqlalchemy.types.SmallInteger(),
            'total_points_won': sqlalchemy.types.SmallInteger(),
            'last_10_balls': sqlalchemy.types.SmallInteger(),
            'match_points_saved': sqlalchemy.types.SmallInteger(),
            'service_games_won': sqlalchemy.types.SmallInteger(),
            'return_games_won': sqlalchemy.types.SmallInteger(),
            'total_games_won': sqlalchemy.types.SmallInteger()
        }
        
        for col in df.columns:
            if col not in dtypes:
                dtypes[col] = sqlalchemy.types.VARCHAR(50) # Fallback
        return dtypes
        
    return {}

def to_sql_optional(tabelas: dict[str, pd.DataFrame], if_exists: str = "append") -> None:
    engine = _make_engine_from_env()

    table_names_sorted = ['jogador', 'Torneio', 'partida', 'PartidaXJogador', 'EstatisticasPartidaXJogador']

    with engine.begin() as conn: 
        for nome_tabela in table_names_sorted:
            if nome_tabela not in tabelas:
                print(f"[DB] Tabela '{nome_tabela}' não encontrada nos dados processados. Pulando.")
                continue
                
            df = tabelas[nome_tabela]
            
            if df is None or df.empty:
                print(f"[DB] '{nome_tabela}' está vazio. Pulando.")
                continue
            
            print(f"[DB] Processando tabela: '{nome_tabela}'...")

            df_para_inserir = df.replace({pd.NaT: None, np.nan: None})
            
            table_dtypes = _get_table_dtypes(nome_tabela, df_para_inserir)

            for col_name in df_para_inserir.columns:
                if col_name in table_dtypes:
                    col_type = table_dtypes[col_name]
                    if isinstance(col_type, (sqlalchemy.types.String, sqlalchemy.types.TEXT, sqlalchemy.types.VARCHAR, sqlalchemy.types.NVARCHAR, sqlalchemy.types.NCHAR)):
                        df_para_inserir[col_name] = df_para_inserir[col_name].fillna('')

            
            pk_col = None
            if nome_tabela == "jogador":
                pk_col = "jogador_id"
            elif nome_tabela == "Torneio":
                pk_col = "torneio_id"
            elif nome_tabela == "partida":
                pk_col = "partida_id"

            if pk_col:
                try:
                    existing_ids_df = pd.read_sql(f"SELECT {pk_col} FROM dbo.{nome_tabela}", conn, coerce_float=False)
                    existing_ids = set(existing_ids_df[pk_col].astype(str))
                except Exception as e:
                    if "Invalid object name" in str(e) or "does not exist" in str(e):
                         print(f"[DB] Tabela dbo.{nome_tabela} não encontrada. Será criada...")
                         existing_ids = set()
                    else:
                         print(f"[DB] Erro ao checar IDs existentes para {nome_tabela}: {e}")
                         existing_ids = set() 

                df_para_inserir = df_para_inserir[~df_para_inserir[pk_col].astype(str).isin(existing_ids)].copy()

                if df_para_inserir.empty:
                    print(f"[DB] '{nome_tabela}': Nenhum dado novo para adicionar.")
                    continue
                        
                print(f"[DB] Subindo '{nome_tabela}' (NOVOS={len(df_para_inserir)}) -> if_exists=append")
            
            else:
                print(f"[DB] Subindo '{nome_tabela}' (linhas={len(df_para_inserir)}) -> if_exists={if_exists}")

            df_para_inserir.to_sql(
                name=nome_tabela,
                con=conn, 
                schema="dbo",
                if_exists="append", 
                index=False,
                dtype=table_dtypes 
            )


# =========================
# Pipeline
# =========================
def main() -> int:
    p = argparse.ArgumentParser(description="ETL simplificado (fetch -> normaliza -> CSV final -> opcional DB)")
    p.add_argument("--date", required=True, help="Data alvo (YYYY-MM-DD). Ex.: 2025-10-27")
    p.add_argument("--sort-cols", action="store_true", help="Repasse para o fetch ordenar colunas")
    p.add_argument("--db", action="store_true", help="Se presente, sobe as tabelas normalizadas para o banco (append)")
    args = p.parse_args()

    _ensure_dirs()

    print("\n==============================")
    print("INÍCIO DA ROTINA ETL (SIMPLIFICADA)")
    print("==============================\D")

    # 1) EXTRAÇÃO (API -> CSV bruto em data/raw)
    print("[1/2] EXTRAÇÃO (API → CSV bruto)")
    fetch_cmd = ["python", FETCH, "--date", args.date]
    if args.sort_cols:
        fetch_cmd.append("--sort-cols")
    _run(fetch_cmd)
    print("[EXTRACT] Coleta concluída.\n")

    # 2) NORMALIZAÇÃO (fixtures bruto -> tabelas finais)
    print("[2/2] NORMALIZAÇÃO (fixtures → tabelas normalizadas)")

    fixtures_csv = _latest_csv(RAW_DIR, "fixtures_*.csv")
    if not fixtures_csv:
        print("[ERRO] Nenhum fixtures_*.csv encontrado em data/raw. A extração falhou?")
        return 1

    print(f"[LOAD] Lendo fixtures: {fixtures_csv.name}")
    df_fixtures = carregar_csv(str(fixtures_csv))

    tabelas = converter_fixtures_em_tabelas(df_fixtures)
    salvar_tabelas_csv(tabelas)

    if args.db:
        try:
            engine = _make_engine_from_env()
            with engine.begin() as conn: # Usar transação explícita
                print("[DB] Limpando tabelas de fatos (partida, PartidaXJogador, EstatisticasPartidaXJogador)...")
                # Começa pelas tabelas com chaves estrangeiras
                conn.execute(sqlalchemy.text("IF OBJECT_ID('dbo.EstatisticasPartidaXJogador', 'U') IS NOT NULL DELETE FROM dbo.EstatisticasPartidaXJogador;"))
                conn.execute(sqlalchemy.text("IF OBJECT_ID('dbo.PartidaXJogador', 'U') IS NOT NULL DELETE FROM dbo.PartidaXJogador;"))
                conn.execute(sqlalchemy.text("IF OBJECT_ID('dbo.partida', 'U') IS NOT NULL DELETE FROM dbo.partida;"))
            
            print("[DB] Inserindo dados...")
            to_sql_optional(tabelas, if_exists="append")
            
        except Exception as e:
            print(f"[WARN] Falha ao subir para o banco: {e}")
            print("     Os CSVs normalizados estão em data/final; você pode carregar manualmente.")

    print("\n[FIM] ETL simplificado concluído com sucesso.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())