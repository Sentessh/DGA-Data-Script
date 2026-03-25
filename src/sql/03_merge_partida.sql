CREATE OR ALTER PROCEDURE dbo.merge_partida
    @data_ref DATE
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.Partida AS tgt
    USING (
        SELECT
            m.event_key,
            t.id_torneio,
            m.event_date,
            m.round
        FROM stg_api.matches_raw m
        JOIN dbo.Torneio_Event t
            ON t.id_torneio_API = m.tournament_key
        WHERE m.data_referencia = @data_ref
    ) AS src
    ON tgt.id_partida_API = src.event_key

    WHEN NOT MATCHED THEN
        INSERT (
            id_partida_API,
            id_torneio,
            partida_data,
            round
        )
        VALUES (
            src.event_key,
            src.id_torneio,
            src.event_date,
            src.round
        );
END;
GO