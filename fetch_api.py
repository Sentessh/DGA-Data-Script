import os
import time
import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

# -------------------- Config --------------------
load_dotenv()

API_BASE = os.getenv("API_BASE", "https://api.api-tennis.com/tennis/")
API_KEY = os.getenv("API_KEY", "")
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./data"))
TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "20"))
SLEEP = float(os.getenv("REQUEST_SLEEP_SECONDS", "0.6"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# -------------------- Helpers --------------------
def call_api(method: str, **params):
    if not API_KEY:
        raise RuntimeError("API_KEY não configurada no .env")

    q = {"method": method, "APIkey": API_KEY}
    q.update({k: v for k, v in params.items() if v is not None})

    print(f"[REQ] method={method} params={q}")
    r = requests.get(API_BASE, params=q, timeout=TIMEOUT)
    r.raise_for_status()
    payload = r.json()

    if payload.get("success") != 1:
        raise RuntimeError(
            f"API retornou erro/sucesso!=1 para {method}. Corpo: {json.dumps(payload)[:500]}"
        )

    return payload.get("result", [])


def to_df(records):
    if not records:
        return pd.DataFrame()
    try:
        return pd.json_normalize(records, max_level=2)
    except Exception:
        return pd.DataFrame(records)


def soft_parse(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    for cand in ("date", "date_start", "date_stop", "match_time", "time", "utc_timestamp", "updated_at"):
        if cand in df.columns:
            df[cand] = pd.to_datetime(df[cand], errors="coerce", utc=True)
    for cand in ("odd", "odds", "price", "prob", "stake"):
        if cand in df.columns:
            df[cand] = pd.to_numeric(df[cand], errors="coerce")
    return df


def _stable_columns(
    df: pd.DataFrame,
    prefer_first=(
        "event_id",
        "match_id",
        "fixture_id",
        "tournament_key",
        "player_key",
        "date",
        "date_start",
        "date_stop",
        "time",
        "utc_timestamp",
    ),
):
    if df.empty:
        return df
    cols = list(df.columns)
    first = [c for c in prefer_first if c in cols]
    the_rest = sorted([c for c in cols if c not in first])
    return df[first + the_rest]


def save_csv(df: pd.DataFrame, name: str):
    ts = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H%M%SZ")
    path = OUTPUT_DIR / f"{name}_{ts}.csv"
    df.to_csv(path, index=False)
    return path


def log_ok(tag, n, path):
    print(f"[OK] {tag:<10} | registros: {n:<6} | arquivo: {path.name}")


def log_empty(tag):
    print(f"[AVISO] {tag:<10} | 0 registros. Nada salvo.")

# -------------------- Main --------------------
def main():
    p = argparse.ArgumentParser(
        description="Coletor mínimo e síncrono da API (players, fixtures, odds) -> CSV"
    )
    p.add_argument("--players", action="store_true", help="Coletar players")
    p.add_argument("--fixtures", action="store_true", help="Coletar fixtures")
    p.add_argument("--odds", action="store_true", help="Coletar odds")
    p.add_argument("--player_key", type=str, help="Filtrar um player específico")
    p.add_argument("--tournament_key", type=str, help="Filtrar por torneio")
    p.add_argument("--date", type=str, help="Data única (ex.: 2025-10-22 ou 22.10.2025)")
    p.add_argument("--date-start", dest="date_start", type=str, help="Início do intervalo")
    p.add_argument("--date-stop", dest="date_stop", type=str, help="Fim do intervalo")
    p.add_argument(
        "--sort-cols",
        action="store_true",
        help="Ordenar colunas alfabeticamente (fallback; além da ordem estável padrão)",
    )

    args = p.parse_args()
    run_all = not (args.players or args.fixtures or args.odds)
    collected_any = False

    try:
        # --- Players ---
        if args.players or run_all:
            if not args.player_key:
                print("[INFO] get_players requer --player_key. Pulando coleta de players. Ex.: --players --player_key 1905")
            else:
                rec = call_api("get_players", player_key=args.player_key)
                df = to_df(rec)
                df = soft_parse(df)
                if args.sort_cols and not df.empty:
                    df = df.reindex(sorted(df.columns), axis=1)
                df = _stable_columns(df)
                if len(df):
                    path = save_csv(df, "players")
                    log_ok("players", len(df), path)
                    collected_any = True
                else:
                    log_empty("players")
                time.sleep(SLEEP)

        # --- Fixtures ---
        if args.fixtures or run_all:
            date_start = args.date_start or args.date
            date_stop = args.date_stop or args.date

            if not date_start or not date_stop:
                print("[INFO] get_fixtures requer --date-start e --date-stop (ou --date). Pulando fixtures.")
            else:
                params = {"date_start": date_start, "date_stop": date_stop}
                if args.tournament_key:
                    params["tournament_key"] = args.tournament_key

                rec = call_api("get_fixtures", **params)
                df = to_df(rec)
                df = soft_parse(df)
                if args.sort_cols and not df.empty:
                    df = df.reindex(sorted(df.columns), axis=1)
                df = _stable_columns(df)
                if len(df):
                    path = save_csv(df, "fixtures")
                    log_ok("fixtures", len(df), path)
                    collected_any = True
                else:
                    log_empty("fixtures")
                time.sleep(SLEEP)

        # --- Odds ---
        if args.odds or run_all:
            date_start = args.date_start or args.date
            date_stop = args.date_stop or args.date

            if not date_start or not date_stop:
                print("[INFO] get_odds requer --date-start e --date-stop (ou --date). Pulando odds.")
            else:
                params = {"date_start": date_start, "date_stop": date_stop}
                if args.tournament_key:
                    params["tournament_key"] = args.tournament_key

                rec = call_api("get_odds", **params)
                df = to_df(rec)
                df = soft_parse(df)
                if args.sort_cols and not df.empty:
                    df = df.reindex(sorted(df.columns), axis=1)
                df = _stable_columns(df)
                if len(df):
                    path = save_csv(df, "odds")
                    log_ok("odds", len(df), path)
                    collected_any = True
                else:
                    log_empty("odds")
                time.sleep(SLEEP)

    except requests.HTTPError as e:
        print(f"[ERRO HTTP] {e} | status={getattr(e.response, 'status_code', None)}")
        return 1
    except Exception as e:
        print(f"[ERRO] {e}")
        return 1

    if not collected_any:
        print("[FIM] Execução concluída, porém nenhum dado foi coletado desta vez.")
    else:
        print("[FIM] Execução concluída com sucesso.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())