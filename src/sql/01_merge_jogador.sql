CREATE OR ALTER PROCEDURE dbo.merge_jogador
    @data_ref DATE
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.Jogador AS tgt
    USING (
        SELECT
            player_api_id,
            nome_completo
        FROM stg_api.players_raw
        WHERE data_referencia = @data_ref
          AND player_api_id IS NOT NULL
    ) AS src
    ON tgt.id_jogador_API = src.player_api_id

    WHEN MATCHED THEN
        UPDATE SET
            tgt.id_jogador_API = src.player_api_id  -- Atualiza caso já exista pelo player_key

    WHEN NOT MATCHED THEN
        INSERT (id_jogador_API, player_key, nome_completo)
        VALUES (
            src.player_api_id,
            'API-' + CAST(src.player_api_id AS VARCHAR(20)),  -- ✅ Ex: API-58668
            src.nome_completo
        );
END;
GO