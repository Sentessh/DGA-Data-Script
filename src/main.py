import argparse
import subprocess
import sys
from datetime import datetime

def run(cmd: list[str]):
    print(f"\n▶ Executando: {' '.join(cmd)}")
    result = subprocess.run(cmd, text=True)

    if result.returncode != 0:
        print("Erro ao executar comando")
        sys.exit(result.returncode)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        help="Data de referência no formato YYYY-MM-DD (default: hoje)"
    )
    parser.add_argument(
        "--db",
        action="store_true",
        help="Carrega staging no banco"
    )
    args = parser.parse_args()

    data_ref = args.date or datetime.today().strftime("%Y-%m-%d")

    print(f"\n🚀 PIPELINE DGA_WTA — DATA {data_ref}")

    run([sys.executable, "src/fetch_api.py", "--date", data_ref])

    # ✅ extract.py já engloba o etl_runner internamente
    extract_cmd = [sys.executable, "src/extract.py", "--date", data_ref]
    if args.db:
        extract_cmd.append("--db")
    run(extract_cmd)

    if args.db:
        run([sys.executable, "src/sql_runner.py", "--date", data_ref])

    print("\n✅ PIPELINE FINALIZADO COM SUCESSO")

if __name__ == "__main__":
    main()