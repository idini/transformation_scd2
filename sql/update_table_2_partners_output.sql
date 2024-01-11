BEGIN

    -- begin bigquery transaction
    BEGIN TRANSACTION;

    -- create temp table where we store rows to insert or update
    CREATE OR REPLACE TEMP TABLE `Table_2_Partners_SCD2`
    AS
    with src_table as (
    select
        PartnerID, Name, Canton
    from `transformation_scd2.table_1_partners_input`
    ),
    dest_table as (
    select
        PartnerID, Name, Canton
    from `transformation_scd2.table_2_partners_output`
    ),
    rows_to_update as (
    select *
    from src_table
    except distinct
    select *
    from dest_table
    )
    select PartnerID, Name, Canton
    from rows_to_update;

    -- update records in table 2 from temp table, if any, setting is_valid to no and date_to to current_datetime
    update `transformation_scd2.table_2_partners_output`
    set Is_valid = 'no',
        Date_To = current_datetime()
    where PartnerID in (
    select PartnerID
    from `Table_2_Partners_SCD2`
    );

    -- insert new records in partners_output
    INSERT INTO `transformation_scd2.table_2_partners_output`
    select
    CAST(FLOOR(1000000*RAND()) AS INT64) AS TechnicalKey,
    PartnerID,
    Name,
    Canton,
    current_datetime(),
    cast('9999-12-31 00:00:00' as datetime),
    'yes'
    from `Table_2_Partners_SCD2`;

    COMMIT TRANSACTION;

    DROP TABLE IF EXISTS `Table_2_Partners_SCD2`;

EXCEPTION WHEN ERROR THEN
    -- in case of failure, rollback transaction
    SELECT @@error.message;
    ROLLBACK TRANSACTION;
END;