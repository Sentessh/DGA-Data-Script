CREATE OR ALTER PROCEDURE dbo.merge_partida_x_jogador
    @data_ref DATE
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.partida_x_jogador AS tgt
    USING (
        SELECT
            m.event_key           AS id_partida_API,
            j.id_jogador          AS id_jogador,
            CASE
                WHEN m.winner = 'First Player'
                     AND j.id_jogador_API = m.first_player_key THEN 1
                WHEN m.winner = 'Second Player'
                     AND j.id_jogador_API = m.second_player_key THEN 1
                ELSE 0
            END AS foi_vencedor
        FROM stg_wta.matches_raw m
        JOIN dbo.jogador j
            ON j.id_jogador_API IN (m.first_player_key, m.second_player_key)
        WHERE m.data_referencia = @data_ref
    ) AS src
    ON tgt.id_partida_API = src.id_partida_API
   AND tgt.id_jogador = src.id_jogador

    WHEN NOT MATCHED THEN
        INSERT (id_partida_API, id_jogador, foi_vencedor)
        VALUES (src.id_partida_API, src.id_jogador, src.foi_vencedor);
END;
GO