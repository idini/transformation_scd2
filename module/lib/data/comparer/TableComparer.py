import logging
from lib.tablemanagement.BigQueryManager import BigQueryManager
from pandas import DataFrame


class TableComparer():
    """
    Class that implements comparation between source table and destination table on Google Bigquery.
    Destination table has TechnicalKey, Date_From, Date_To, Is_valid as tech columns.
    Schema for source table and destination table should be the same, except for tech fields in destination table.

    Args:
        bigquery_manager (BigQueryManager): table manager for Bigquery.

    """

    DIFF_STMT = """
        with src_table as (
            select *
            from `{src_table}`
        ),
        dest_table as (
            select * except(TechnicalKey, Date_From, Date_To, Is_valid)
            from `{dest_table}`
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
    """

    def __init__(self,
                 bigquery_manager:BigQueryManager
                ) -> None:
        self.__bigquery_manager = bigquery_manager
        self.__logger = logging.getLogger()

    def compare_tables(self, src_table:str, dest_table:str) -> DataFrame:
        """ method used to check differences between source and destination tables on bigquery
            based on SQL statement that returns a pandas DataFrame containing the new, updated and deleted rows

        Args:
            src_table (str): name of source table
            dest_table (str): name of destination table

        Returns:
            pandas.DataFrame: pandas.DataFrame containing the new or updated rows

        Notes:
            the dataframe will contains all fields from source/destination table and a column 'operation'
            where value 1 means new or updated rows (to insert in the destination table)
            and value 2 means deleted rows (to invalidate in the destination table)

        """
        sql_difference_query = TableComparer.DIFF_STMT.format(
                src_table = src_table,
                dest_table = dest_table
            )

        return self.__bigquery_manager.run_query(sql_difference_query)



