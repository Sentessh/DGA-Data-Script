import os
import time
import argparse
import datetime as dt
from pathlib import Path
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

API_BASE = "https://api.api-tennis.com/tennis/"
API_KEY = os.getenv("TENNIS_API_KEY")
RAW_DIR = Path("./data/raw")
SLEEP = 0.6

RAW_DIR.mkdir(parents=True, exist_ok=True)

def call_api(method: str, **params):
    if not API_KEY:
        raise RuntimeError("TENNIS_API_KEY não encontrada no .env")
    q = {"method": method, "APIkey": API_KEY}
    q.update({k: v for k, v in params.items() if v is not None})
    r = requests.get(API_BASE, params=q, timeout=20.0)
    r.raise_for_status()
    payload = r.json()
    return payload.get("result", []) if payload.get("success") == 1 else []

def extrair_estatisticas(event):
    registros = []
    stats = event.get("statistics", [])
    for s in stats:
        registros.append({
            "event_key": event.get("event_key"),
            "player_key": s.get("player_key"),
            "stat_name": s.get("stat_name"),
            "stat_value": s.get("stat_value")
        })
    return registros

def run_fetch(date: str, event_type: str = "WTA"):

    estatisticas = []

    rec_fix = call_api("get_fixtures", date_start=date, date_stop=date)

    if rec_fix:
        df = pd.json_normalize(rec_fix)

        ts = dt.datetime.now().strftime("%Y%m%d%H%M%S")  # ✅ Corrigido: incluído %m%d
        df.to_csv(RAW_DIR / f"fixtures_{date}_{ts}.csv", index=False)
        print(f"[OK] Fixtures: {len(df)} registros.")

        for event in rec_fix:
            estatisticas.extend(extrair_estatisticas(event))

        if estatisticas:
            df_stats = pd.DataFrame(estatisticas)
            df_stats.to_csv(RAW_DIR / f"stats_{date}_{ts}.csv", index=False)
            print(f"[OK] Estatísticas: {len(df_stats)} registros.")
        else:
            print("[INFO] Nenhuma estatística disponível para esta data.")

    time.sleep(SLEEP)

    rec_stand = call_api("get_standings", event_type=event_type)

    if rec_stand:
        df = pd.json_normalize(rec_stand)
        ts = dt.datetime.now().strftime("%Y%m%d%H%M%S")  # ✅ Corrigido: incluído %m%d
        df.to_csv(RAW_DIR / f"standings_{date}_{ts}.csv", index=False)
        print(f"[OK] Standings: {len(df)} registros.")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--type", default="WTA")
    args = p.parse_args()

    run_fetch(args.date, args.type)