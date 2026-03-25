CREATE OR ALTER PROCEDURE dbo.merge_ranking_jogador
    @data_ref DATE
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.Ranking (
        id_jogador,
        rank,
        points,
        ranking_date
    )
    SELECT
        j.id_jogador,
        r.rank,
        r.points,
        r.ranking_date
    FROM stg_api.rankings_raw r
    JOIN dbo.Jogador j
        ON j.id_jogador_API = r.player_api_id
    WHERE r.data_referencia = @data_ref
      AND NOT EXISTS (
          SELECT 1
          FROM dbo.Ranking x
          WHERE x.id_jogador = j.id_jogador
            AND x.ranking_date = r.ranking_date
      );
END;
GO