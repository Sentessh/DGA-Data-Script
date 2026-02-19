CREATE OR ALTER PROCEDURE dbo.merge_partida
    @data_ref DATE
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.partida AS tgt
    USING (
        SELECT
            event_key,
            tournament_key,
            event_date,
            round
        FROM stg_wta.matches_raw
        WHERE data_referencia = @data_ref
    ) AS src
    ON tgt.id_partida_API = src.event_key

    WHEN NOT MATCHED THEN
        INSERT (
            id_partida_API,
            id_torneio,
            data_partida,
            fase
        )
        VALUES (
            src.event_key,
            src.tournament_key,
            src.event_date,
            src.round
        );
END;
GO