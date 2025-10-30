import subprocess
from pathlib import Path

def extract_all(date: str):

    print(f"\n[EXTRACT] Iniciando coleta de dados para {date}...\n")

    script_path = Path(__file__).parent / "fetch_api.py"
    cmd = [
        "python",
        str(script_path),
        "--date", date,
        "--sort-cols"
    ]

    try:
        subprocess.run(cmd, check=True)
        print("\n[EXTRACT] Coleta concluída com sucesso.")
    except subprocess.CalledProcessError as e:
        print(f"[ERRO] Falha ao executar fetch_api.py: {e}")
        raise