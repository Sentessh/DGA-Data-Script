import os
import pandas as pd
import numpy as np
from urllib.parse import quote_plus
import sqlalchemy
from dotenv import load_dotenv

load_dotenv()

URL_PLAYERS = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_players.csv"

def get_engine():
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
            f"DRIVER={driver};SERVER={host},{port};DATABASE={name};"
            f"UID={user};PWD={pwd};Encrypt=yes;TrustServerCertificate=yes;MARS_Connection=yes;"
        )
        connect_str = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"
    
    return sqlalchemy.create_engine(connect_str)

def main():
    print("=== SINCRONIZAÇÃO SACKMANN (JOGADORES) ===")
    print("Estratégia: Converter 'Alexandre Akli' -> 'A. Akli' para matching.")
    
    # 1. Baixar dados
    print(f"[1/3] Baixando {URL_PLAYERS}...")
    try:
        df_sack = pd.read_csv(URL_PLAYERS, low_memory=False)
    except Exception as e:
        print(f"Erro ao baixar: {e}")
        return

    # 2. Preparar dados
    print(f"[2/3] Preparando {len(df_sack)} jogadores...")
    
    # Limpeza básica de strings
    df_sack['name_first'] = df_sack['name_first'].astype(str).fillna('').str.strip()
    df_sack['name_last'] = df_sack['name_last'].astype(str).fillna('').str.strip()
    
    df_sack['dob_clean'] = pd.to_datetime(df_sack['dob'], format='%Y%m%d', errors='coerce')
    
    # Selecionar colunas (MUDANÇA: enviamos first e last separados)
    df_stage = df_sack[[
        'player_id', 
        'name_first',
        'name_last', 
        'dob_clean', 
        'hand', 
        'ioc', 
        'height'
    ]].copy()

    df_stage.columns = ['s_id', 's_first', 's_last', 's_dob', 's_hand', 's_ioc', 's_height']

    # 3. Subir para Banco e Cruzar
    engine = get_engine()
    print("[3/3] Atualizando Banco de Dados (Lotes de 200)...")
    
    try:
        with engine.begin() as conn:
            # Sobe a tabela temporária
            df_stage.to_sql('stage_sackmann_players', conn, schema='dbo', if_exists='replace', index=False, chunksize=200)
            print("   > Tabela 'stage_sackmann_players' criada.")
            
            # Compara: "a. akli" == (primeira letra de "alexandre") + ". " + "akli"
            sql_update = """
            UPDATE j
            SET 
                j.id_jogador_Sackmann = s.s_id,
                j.altura = COALESCE(j.altura, s.s_height),
                j.nacionalidade = COALESCE(j.nacionalidade, s.s_ioc),
                j.mao_dominante = COALESCE(j.mao_dominante, s.s_hand),
                j.data_nascimento = COALESCE(j.data_nascimento, s.s_dob)
            FROM dbo.Jogador j
            INNER JOIN dbo.stage_sackmann_players s
            ON 
                -- Lógica de Matching: Inicial + Ponto + Espaço + Sobrenome
                -- Ex: 'A. Akli' bate com 'Alexandre' + 'Akli'
                LOWER(j.nome_completo) = LOWER(LEFT(s.s_first, 1) + '. ' + s.s_last)
            WHERE j.id_jogador_Sackmann IS NULL;
            """
            
            print("   > Executando cruzamento (MATCH por inicial)...")
            result = conn.execute(sqlalchemy.text(sql_update))
            rows_affected = result.rowcount
            
            # Limpeza
            conn.execute(sqlalchemy.text("DROP TABLE dbo.stage_sackmann_players"))
            
            print(f"   > SUCESSO: {rows_affected} jogadores vinculados!")
            
    except Exception as e:
        print(f"ERRO NO SQL: {e}")

if __name__ == "__main__":
    main()