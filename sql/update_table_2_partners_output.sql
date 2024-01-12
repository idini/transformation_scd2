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
            where Is_valid = 'yes'
        ),
        rows_to_update as (
            select *
            from src_table
            except distinct
            select *
            from dest_table
        ),
        rows_to_delete as (
            select *
            from dest_table
            except distinct
            select *
            from src_table
        )
        select *, 1 as operation # new/updated
        from rows_to_update
        union all
        select *, 2 as operation # deleted
        from rows_to_delete;



    -- update records in table 2 from temp table, if any, setting is_valid to 'no' and date_to to previous day
    update `transformation_scd2.table_2_partners_output`
    set Is_valid = 'no',
        Date_To = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    where PartnerID in (
    select distinct PartnerID
    from `Table_2_Partners_SCD2`
    );

    -- add new record for each change in partners_output
    INSERT INTO `transformation_scd2.table_2_partners_output`
    select
    CAST(FLOOR(1000000*RAND()) AS INT64) AS TechnicalKey,
    PartnerID,
    Name,
    Canton,
    current_date(),
    cast('9999-12-31 00:00:00' as datetime),
    'yes'
    from `Table_2_Partners_SCD2`
    where operation = 1;

    COMMIT TRANSACTION;

    DROP TABLE IF EXISTS `Table_2_Partners_SCD2`;

EXCEPTION WHEN ERROR THEN
    -- in case of failure, rollback transaction
    SELECT @@error.message;
    ROLLBACK TRANSACTION;
END;