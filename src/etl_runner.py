import argparse
from extract import extract_all
from transform import transform_all
from load import upload_to_db

def main():
    parser = argparse.ArgumentParser(
        description="Pipeline ETL: API → CSV bruto → CSV final → SQL Server"
    )
    parser.add_argument("--date", required=True, help="Data alvo (YYYY-MM-DD)")
    parser.add_argument(
        "--load-mode",
        choices=["replace", "append"],
        default="replace",
        help="Como gravar no banco remoto: replace = sobrescreve / append = histórico",
    )

    args = parser.parse_args()

    print("\n==============================")
    print("INÍCIO DA ROTINA ETL")
    print("==============================")

    # 1. EXTRAÇÃO
    print("\n[1/3] EXTRAÇÃO (API → CSV bruto)")
    extract_all(args.date)

    # 2. TRANSFORMAÇÃO
    print("\n[2/3] TRANSFORMAÇÃO (limpeza → CSV final)")
    transform_all()

    # 3. CARGA
    print("\n[3/3] CARGA (CSV final → SQL Server)")
    upload_to_db(mode=args.load_mode)

    print("\n==============================")
    print("ETL FINALIZADO COM SUCESSO")
    print("==============================\n")

if __name__ == "__main__":
    main()