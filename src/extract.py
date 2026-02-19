import subprocess
from pathlib import Path

def extract_all(date: str):
    print(f"\n[EXTRACT] Coletando dados para {date}...")
    script_path = Path(__file__).parent / "fetch_api.py"

    cmd = ["python", str(script_path), "--date", date]

    try:
        subprocess.run(cmd, check=True)
        print("[EXTRACT] Sucesso.")
    except Exception as e:
        print(f"[ERRO] Falha: {e}")