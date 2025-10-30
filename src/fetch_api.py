import os
import time
import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd
import requests

API_BASE = "https://api.api-tennis.com/tennis/"
API_KEY = "931591825e219e56d07e0d0c49d24cb7c0672fc09dc0e6b12f2c7920a3b51dfc"  # <-- coloque sua chave válida aqui

RAW_DIR = Path("./data/raw")

TIMEOUT = 20.0

SLEEP = 0.6

RAW_DIR.mkdir(parents=True, exist_ok=True)

def call_api(method: str, **params):

    if not API_KEY:
        raise RuntimeError("API_KEY não definida dentro do script.")

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

    for cand in (
        "date",
        "date_start",
        "date_stop",
        "match_time",
        "time",
        "utc_timestamp",
        "updated_at",
    ):
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


def _timestamp_utc():

    return dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H%M%SZ")


def save_csv(df: pd.DataFrame, name: str) -> Path:

    ts = _timestamp_utc()
    path = RAW_DIR / f"{name}_{ts}.csv"
    df.to_csv(path, index=False)
    return path


def log_ok(tag: str, n: int, path: Path):
    print(f"[OK] {tag:<10} | registros: {n:<6} | arquivo: {path.name}")


def log_empty(tag: str):
    print(f"[AVISO] {tag:<10} | 0 registros. Nada salvo.")


def run_fetch(
    date: str,
    tournament_key: str | None = None,
    player_key: str | None = None,
    sort_cols: bool = True,
):

    collected_paths = []
    
    if player_key:
        try:
            rec = call_api("get_players", player_key=player_key)
            df = to_df(rec)
            df = soft_parse(df)
            if sort_cols and not df.empty:
                df = df.reindex(sorted(df.columns), axis=1)
            df = _stable_columns(df)

            if len(df):
                p = save_csv(df, "players")
                log_ok("players", len(df), p)
                collected_paths.append(p)
            else:
                log_empty("players")

            time.sleep(SLEEP)

        except Exception as e:
            print(f"[WARN] Falha em players: {e}")

    try:
        fixtures_params = {"date_start": date, "date_stop": date}
        if tournament_key:
            fixtures_params["tournament_key"] = tournament_key

        rec = call_api("get_fixtures", **fixtures_params)
        df = to_df(rec)
        df = soft_parse(df)
        if sort_cols and not df.empty:
            df = df.reindex(sorted(df.columns), axis=1)
        df = _stable_columns(df)

        if len(df):
            p = save_csv(df, "fixtures")
            log_ok("fixtures", len(df), p)
            collected_paths.append(p)
        else:
            log_empty("fixtures")

        time.sleep(SLEEP)

    except Exception as e:
        print(f"[WARN] Falha em fixtures: {e}")


    try:
        odds_params = {"date_start": date, "date_stop": date}
        if tournament_key:
            odds_params["tournament_key"] = tournament_key

        rec = call_api("get_odds", **odds_params)
        df = to_df(rec)
        df = soft_parse(df)
        if sort_cols and not df.empty:
            df = df.reindex(sorted(df.columns), axis=1)
        df = _stable_columns(df)

        if len(df):
            p = save_csv(df, "odds")
            log_ok("odds", len(df), p)
            collected_paths.append(p)
        else:
            log_empty("odds")

        time.sleep(SLEEP)

    except Exception as e:
        print(f"[WARN] Falha em odds: {e}")

    print("[FETCH] Concluído.")
    return [str(p) for p in collected_paths]


def main():
    p = argparse.ArgumentParser(
        description="Coletor mínimo e síncrono da API (players, fixtures, odds) -> CSV bruto em data/raw"
    )
    p.add_argument("--date", required=True, help="Data única (ex.: 2025-10-27)")
    p.add_argument("--player_key", type=str, help="Filtrar um player específico (players)")
    p.add_argument("--tournament_key", type=str, help="Filtrar por torneio (fixtures/odds)")
    p.add_argument(
        "--sort-cols",
        action="store_true",
        help="Ordenar colunas alfabeticamente (além da ordem estável prioritária)",
    )
    args = p.parse_args()

    try:
        run_fetch(
            date=args.date,
            tournament_key=args.tournament_key,
            player_key=args.player_key,
            sort_cols=args.sort_cols,
        )
    except requests.HTTPError as e:
        print(f"[ERRO HTTP] {e} | status={getattr(e.response, 'status_code', None)}")
        return 1
    except Exception as e:
        print(f"[ERRO] {e}")
        return 1

    print("[FIM] Execução concluída.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())