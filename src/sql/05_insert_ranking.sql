CREATE OR ALTER PROCEDURE dbo.merge_ranking_jogador
    @data_ref DATE
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.ranking_jogador (
        id_jogador,
        posicao,
        pontos,
        data_referencia
    )
    SELECT
        j.id_jogador,
        r.rank,
        r.points,
        r.ranking_date
    FROM stg_wta.rankings_raw r
    JOIN dbo.jogador j
        ON j.id_jogador_API = r.player_api_id
    WHERE r.data_referencia = @data_ref
      AND NOT EXISTS (
          SELECT 1
          FROM dbo.ranking_jogador x
          WHERE x.id_jogador = j.id_jogador
            AND x.data_referencia = r.ranking_date
      );
END;
GO