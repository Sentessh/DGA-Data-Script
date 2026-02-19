CREATE OR ALTER PROCEDURE dbo.merge_torneio
    @data_ref DATE
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.torneio AS tgt
    USING (
        SELECT DISTINCT
            tournament_key,
            tournament_name,
            event_date
        FROM stg_wta.matches_raw
        WHERE data_referencia = @data_ref
    ) AS src
    ON tgt.id_torneio_API = src.tournament_key

    WHEN NOT MATCHED THEN
        INSERT (id_torneio_API, nome_descricao, data)
        VALUES (src.tournament_key, src.tournament_name, src.event_date);
END;
GO