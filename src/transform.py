import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = os.getenv("RAW_DIR", "./data/raw")
PROCESSED_DIR = os.getenv("PROCESSED_DIR", "./data/processed")

os.makedirs(PROCESSED_DIR, exist_ok=True)

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:

    df.columns = [c.strip().lower() for c in df.columns]

    df = df.drop_duplicates()

    for col in df.columns:
        if any(k in col for k in ["date", "time", "timestamp"]):
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    numeric_hints = ["odd", "odds", "price", "prob", "stake", "pct", "line"]
    for col in df.columns:
        if any(h in col for h in numeric_hints):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

def transform_one_file(raw_path: str) -> str:
    df = pd.read_csv(raw_path)
    df = clean_dataframe(df)

    base_name = os.path.basename(raw_path)
    out_name = f"final_{base_name}"
    out_path = os.path.join(PROCESSED_DIR, out_name)

    df.to_csv(out_path, index=False)
    print(f"[TRANSFORM] {base_name} -> {out_name} ({len(df)} linhas)")
    return out_path

def transform_all() -> list[str]:
    print("[TRANSFORM] Iniciando transformação dos CSVs brutos...")
    outputs = []
    for filename in os.listdir(RAW_DIR):
        if filename.endswith(".csv"):
            raw_path = os.path.join(RAW_DIR, filename)
            out_path = transform_one_file(raw_path)
            outputs.append(out_path)
    print("[TRANSFORM] Concluído.")
    return outputs