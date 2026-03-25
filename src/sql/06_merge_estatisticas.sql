CREATE OR ALTER PROCEDURE dbo.merge_estatisticas_partida
    @data_ref DATE
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.EstatisticasJogador_Partida AS tgt
    USING (
        SELECT
            pj.id_partida,
            pj.id_jogador,
            MAX(CASE WHEN s.stat_name = 'Aces'                    THEN TRY_CAST(s.stat_value AS INT) END) AS aces,
            MAX(CASE WHEN s.stat_name = 'Double Faults'           THEN TRY_CAST(s.stat_value AS INT) END) AS double_faults,
            MAX(CASE WHEN s.stat_name = 'Service Points Won'      THEN TRY_CAST(s.stat_value AS INT) END) AS serve_points_won,
            MAX(CASE WHEN s.stat_name = '1st Serve Points Won'    THEN TRY_CAST(s.stat_value AS INT) END) AS [1st_serve_points_won],
            MAX(CASE WHEN s.stat_name = '2nd Serve Points Won'    THEN TRY_CAST(s.stat_value AS INT) END) AS [2nd_serve_points_won],
            MAX(CASE WHEN s.stat_name = 'Break Points Converted'  THEN TRY_CAST(s.stat_value AS INT) END) AS API_break_points_converted,
            MAX(CASE WHEN s.stat_name = 'Return Points Won'       THEN TRY_CAST(s.stat_value AS INT) END) AS API_return_points_won,
            MAX(CASE WHEN s.stat_name = 'Total Points Won'        THEN TRY_CAST(s.stat_value AS INT) END) AS API_total_points_won
        FROM stg_api.match_stats_raw s
        JOIN dbo.Partida p
            ON p.id_partida_API = s.match_api_id
        JOIN dbo.Jogador j
            ON j.id_jogador_API = s.player_api_id
        JOIN dbo.Partida_Jogador pj
            ON pj.id_partida = p.id_partida
           AND pj.id_jogador = j.id_jogador
        WHERE s.data_referencia = @data_ref
        GROUP BY pj.id_partida, pj.id_jogador
    ) AS src
    ON tgt.id_partida = src.id_partida
   AND tgt.id_jogador = src.id_jogador

    WHEN MATCHED THEN
        UPDATE SET
            aces                        = src.aces,
            double_faults               = src.double_faults,
            serve_points_won            = src.serve_points_won,
            [1st_serve_points_won]      = src.[1st_serve_points_won],
            [2nd_serve_points_won]      = src.[2nd_serve_points_won],
            API_break_points_converted  = src.API_break_points_converted,
            API_return_points_won       = src.API_return_points_won,
            API_total_points_won        = src.API_total_points_won

    WHEN NOT MATCHED THEN
        INSERT (
            id_partida,
            id_jogador,
            aces,
            double_faults,
            serve_points_won,
            [1st_serve_points_won],
            [2nd_serve_points_won],
            API_break_points_converted,
            API_return_points_won,
            API_total_points_won
        )
        VALUES (
            src.id_partida,
            src.id_jogador,
            src.aces,
            src.double_faults,
            src.serve_points_won,
            src.[1st_serve_points_won],
            src.[2nd_serve_points_won],
            src.API_break_points_converted,
            src.API_return_points_won,
            src.API_total_points_won
        );
END;
GO