import logging
import random
from datetime import date, datetime, time
from lib.data.comparer.TableComparer import TableComparer
from lib.dbmanagement.connector.BigQueryConnector import BigQueryConnector
from lib.dbmanagement.tablemanagement.BigQueryManager import BigQueryManager
from lib.dbmanagement.transaction.BigqueryTransaction import BigqueryTransaction
from google.cloud.bigquery import QueryJobConfig
from pandas import DataFrame


class DataIngestor():
    """
    Class that implements insertion and updates in the destination table on Google Bigquery following the
    Slowly Changing Dimension Type 2 (see https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row).

    Args:
        bigquery_connection (BigQueryConnector): Bigquery connection from BigQueryConnector.

    """


    def __init__(self,
                 bigquery_connection:BigQueryConnector
            ) -> None:

        self.__logger = logging.getLogger()
        self.__bigquery_manager = BigQueryManager(bigquery_connection)
        self.__comparer = TableComparer(bigquery_connection)
        self.__bigquery_transaction = BigqueryTransaction(bigquery_connection = bigquery_connection)


    def __update_data(self,
                      destination_table:str,
                      pkey:str,
                      data_to_ingest: DataFrame,
                      job_config:QueryJobConfig = None
                      ) -> None:
        """ private method used to update data in destination tables on bigquery based on previously checked
            compared data between source and destination table

        Args:
            destination_table (str): name of destination table
            pkey (str): name of primary (or surrogate) key
            data_to_ingest (DataFrame) : dataframe containing new/updated/deleted records
            job_config (QueryJobConfig, optional): query job used for transaction. If None, method runs without transaction


        Notes:
            Based on latest version of google-cloud library (3.15.0) (https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client)
            method update_rows does not exist anymore

        """
        assignments = dict()
        assignments['Is_valid'] = '"no"'
        assignments['Date_To'] = 'DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)'

        list_pkeys_involved = str(set(data_to_ingest[pkey])).replace('{','').replace('}','')
        filter_cond = f"{pkey} in ({list_pkeys_involved})"

        return self.__bigquery_manager.update_records(destination_table, assignments, filter_cond, job_config)

    def __assign_tech_id(self, technical_id_list:list) -> int:
        """ private method used to generate a new technical id not already stored in table

        Args:
            technical_id_list (list): list of technical id stored in destination table

        Returns:
            int: new technical id
        """

        x = random.randint(100000,999999)
        cnt = 0
        threshold = 10
        while x in technical_id_list:
            print(x, "in",technical_id_list)
            x = random.randint(1,4)
            if cnt > threshold:
                raise Exception("Error generating new Technical ID")
            cnt += 1
        return x

    def __insert_data(self,
                      destination_table: str,
                      data_to_ingest: DataFrame,
                      job_config: QueryJobConfig = None
                      ) -> None:
        """ private method used to insert new data in destination tables on bigquery based on previously checked
            compared data between source and destination table

        Args:
            destination_table (str): name of destination table
            data_to_ingest (DataFrame) : dataframe containing new/updated/deleted record
            job_config (QueryJobConfig, optional): query job used for transaction. If None, method runs without transaction


        Returns:
            pandas.DataFrame: pandas.DataFrame containing the new or updated rows

        Notes:
            the dataframe will contains all fields from source/destination table and a column 'operation'
            where value 1 means new or updated rows (to insert in the destination table)
            and value 2 means deleted rows (to invalidate in the destination table)

        """

        query_job = self.__bigquery_manager.run_query("select distinct TechnicalKey from transformation_scd2.table_2_partners_output")
        technical_id_list = query_job.to_dataframe()

        rows_to_insert = data_to_ingest[data_to_ingest['operation']== 1].drop(columns=['operation'])
        rows_to_insert.insert(loc=0, column='TechnicalKey', value=self.__assign_tech_id(technical_id_list))
        #rows_to_insert['TechnicalKey'] = rows_to_insert.apply(lambda x : self.__assign_tech_id(technical_id_list), axis=1)
        rows_to_insert['Date_To'] = datetime(9999,1,1,0,0,0).isoformat()
        rows_to_insert['Date_From'] = datetime.combine(date.today(), time.min).isoformat()
        rows_to_insert['Is_valid'] = 'yes'

        return self.__bigquery_manager.insert_records(destination_table, rows_to_insert, job_config)


    def ingest_data(self,
                    source_table:str,
                    destination_table:str,
                    pkey:str) -> int:

        """ method used to insert or update  data in destination tables on bigquery based on previously checked
            compared data between source and destination table

        Args:
            source_table (str): name of destination table
            destination_table (str): name of destination table
            pkey (str): name of primary (or surrogate) key

        Notes:
            the dataframe will contains all fields from source/destination table and a column 'operation'
            where value 1 means new or updated rows (to insert in the destination table)
            and value 2 means deleted rows (to invalidate in the destination table)

        """
        try:
            job_config = self.__bigquery_transaction.begin_transaction()

            data_to_ingest = self.__comparer.compare_tables(source_table, destination_table)

            if not data_to_ingest.empty:
                rows_updated  = self.__update_data(destination_table, pkey, data_to_ingest, job_config)
            if not data_to_ingest[data_to_ingest['operation'] == 1].empty:
                rows_inserted = self.__insert_data(destination_table,data_to_ingest, job_config)

            self.__bigquery_transaction.commit_transaction()

        except Exception as error: #rollback
            self.__bigquery_transaction.rollback_transaction()
            self.__logger.error(f'Rollback with error: {error}')
            raise error

        return 0
