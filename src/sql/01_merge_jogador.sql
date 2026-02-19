CREATE OR ALTER PROCEDURE dbo.merge_jogador
    @data_ref DATE
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.jogador AS tgt
    USING (
        SELECT
            player_api_id,
            nome_completo
        FROM stg_wta.players_raw
        WHERE data_referencia = @data_ref
    ) AS src
    ON tgt.id_jogador_API = src.player_api_id

    WHEN NOT MATCHED THEN
        INSERT (id_jogador_API, nome_completo)
        VALUES (src.player_api_id, src.nome_completo);
END;
GO