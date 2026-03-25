import argparse
from sqlalchemy import text
from etl_runner import get_engine

PIPELINE = "DGA_WTA"

PROCEDURES = [
    "dbo.merge_jogador",
    "dbo.merge_torneio",
    "dbo.merge_partida",
    "dbo.merge_partida_x_jogador",
    "dbo.merge_ranking_jogador",
    "dbo.merge_estatisticas_partida",   # ✅ Corrigido: nome consistente com o SQL
]

def main(data_ref):
    engine = get_engine()

    with engine.begin() as conn:

        check = conn.execute(text("""
            SELECT status
            FROM dbo.etl_execucao
            WHERE pipeline = :pipeline
              AND data_referencia = :data_ref
        """), {
            "pipeline": PIPELINE,
            "data_ref": data_ref
        }).fetchone()

        if check and check[0] == "SUCCESS":
            print("⏭ ETL já executado para essa data.")
            return

        conn.execute(text("""
            MERGE dbo.etl_execucao AS tgt
            USING (SELECT :pipeline AS pipeline, :data_ref AS data_ref) src
            ON tgt.pipeline = src.pipeline
           AND tgt.data_referencia = src.data_ref
            WHEN MATCHED THEN
                UPDATE SET status = 'RUNNING', started_at = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (pipeline, data_referencia, status, started_at)
                VALUES (:pipeline, :data_ref, 'RUNNING', GETDATE());
        """), {
            "pipeline": PIPELINE,
            "data_ref": data_ref
        })

        try:
            for proc in PROCEDURES:
                print(f"▶ Executando {proc}...")
                conn.execute(
                    text(f"EXEC {proc} @data_ref = :data_ref"),
                    {"data_ref": data_ref}
                )

            conn.execute(text("""
                UPDATE dbo.etl_execucao
                SET status = 'SUCCESS', finished_at = GETDATE()
                WHERE pipeline = :pipeline
                  AND data_referencia = :data_ref
            """), {
                "pipeline": PIPELINE,
                "data_ref": data_ref
            })

            print("✅ SQL pipeline finalizado com sucesso.")

        except Exception as e:
            conn.execute(text("""
                UPDATE dbo.etl_execucao
                SET status = 'FAILED', finished_at = GETDATE()
                WHERE pipeline = :pipeline
                  AND data_referencia = :data_ref
            """), {
                "pipeline": PIPELINE,
                "data_ref": data_ref
            })
            print("❌ ERRO NO SQL RUNNER")
            raise e


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    main(args.date)