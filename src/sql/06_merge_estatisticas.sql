MERGE dbo.partida_estatistica AS tgt
USING (
    SELECT
        pj.partida_id,
        pj.jogador_id,
        s.aces,
        s.double_faults,
        s.first_serve_pct,
        s.break_points_won,
        s.data_referencia
    FROM stg_partida_estatistica s
    JOIN dbo.partida p
        ON p.event_key = s.event_key
    JOIN dbo.jogador j
        ON j.player_api_id = s.player_key
    JOIN dbo.partida_jogador pj
        ON pj.partida_id = p.partida_id
        AND pj.jogador_id = j.jogador_id
) src
ON tgt.partida_id = src.partida_id
AND tgt.jogador_id = src.jogador_id
AND tgt.data_referencia = src.data_referencia

WHEN MATCHED THEN
    UPDATE SET
        aces = src.aces,
        double_faults = src.double_faults,
        first_serve_pct = src.first_serve_pct,
        break_points_won = src.break_points_won

WHEN NOT MATCHED THEN
    INSERT (
        partida_id,
        jogador_id,
        aces,
        double_faults,
        first_serve_pct,
        break_points_won,
        data_referencia
    )
    VALUES (
        src.partida_id,
        src.jogador_id,
        src.aces,
        src.double_faults,
        src.first_serve_pct,
        src.break_points_won,
        src.data_referencia
    );