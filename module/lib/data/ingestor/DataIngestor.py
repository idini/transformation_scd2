import logging
import random
from lib.tablemanagement.BigQueryManager import BigQueryManager
from pandas import DataFrame
from datetime import datetime

class DataIngestor():
    """
    Class that implements insertion and updates in the destination table on Google Bigquery following the
    Slowly Changing Dimension Type 2 (see https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row).

    Args:
        bigquery_manager (BigQueryManager): table manager for Bigquery.

    """


    def __init__(self,
                 bigquery_manager:BigQueryManager
            ) -> None:
        self.__bigquery_manager = bigquery_manager
        self.__logger = logging.getLogger()


    def __update_data(self,
                      destination_table:str,
                      pkey:str,
                      data_to_ingest: DataFrame
                      ) -> None:
        """ private method used to update data in destination tables on bigquery based on previously checked
            compared data between source and destination table

        Args:
            destination_table (str): name of destination table
            pkey (str): name of primary (or surrogate) key
            data_to_ingest (DataFrame) : dataframe containing new/updated/deleted records

        Notes:
            Based on latest version of google-cloud library (3.15.0) (https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client)
            method update_rows does not exist anymore

        """
        assignments = dict()
        assignments['Is_valid'] = '"no"'
        assignments['Date_To'] = 'DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)'

        list_pkeys_involved = str(set(data_to_ingest[pkey])).replace('{','').replace('}','')
        filter_cond = f"{pkey} in ({list_pkeys_involved})"

        self.__bigquery_manager.update_data(destination_table=destination_table,assignments=assignments, filter_cond=filter_cond)

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
                      destination_table:str,
                      data_to_ingest: DataFrame
                      ) -> None:
        """ private method used to insert new data in destination tables on bigquery based on previously checked
            compared data between source and destination table

        Args:
            destination_table (str): name of destination table
            data_to_ingest (DataFrame) : dataframe containing new/updated/deleted record

        Returns:
            pandas.DataFrame: pandas.DataFrame containing the new or updated rows

        Notes:
            the dataframe will contains all fields from source/destination table and a column 'operation'
            where value 1 means new or updated rows (to insert in the destination table)
            and value 2 means deleted rows (to invalidate in the destination table)

        """
        technical_id_list = self.__bigquery_manager.run_query("select distinct TechnicalKey from transformation_scd2.table_2_partners_output")
        rows_to_insert = data_to_ingest[data_to_ingest['operation']== 1].drop(columns=['operation'])

        rows_to_insert['TechnicalKey'] = rows_to_insert.apply(lambda x : self.__assign_tech_id(technical_id_list), axis=1)
        rows_to_insert['Date_To'] = datetime(9999,1,1,0,0,0).isoformat()
        rows_to_insert['Date_From'] = datetime.now().date().isoformat()
        rows_to_insert['Is_valid'] = 'yes'

        self.__bigquery_manager.insert_data(destination_table=destination_table,rows=rows_to_insert)


    def ingest_data(self,
                    destination_table:str,
                    pkey:str,
                    data_to_ingest:DataFrame) -> None:

        """ method used to insert or update  data in destination tables on bigquery based on previously checked
            compared data between source and destination table

        Args:
            destination_table (str): name of destination table
            pkey (str): name of primary (or surrogate) key
            data_to_ingest (DataFrame) : dataframe containing new/updated/deleted record


        Notes:
            the dataframe will contains all fields from source/destination table and a column 'operation'
            where value 1 means new or updated rows (to insert in the destination table)
            and value 2 means deleted rows (to invalidate in the destination table)

        """
        self.__update_data(destination_table, pkey, data_to_ingest)
        self.__insert_data(destination_table,data_to_ingest)



    UPDATE_STMT = """
    update `{destination_table}`
        set Is_valid = 'no',
            Date_To = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        where {pkey} in ({list_pkeys_involved});
    """

    def __update_data_old(self,
                      destination_table:str,
                      pkey:str,
                      data_to_ingest: DataFrame
                      ) -> None:
        """ private method used to update data in destination tables on bigquery based on previously checked
            compared data between source and destination table

        Args:
            destination_table (str): name of destination table
            pkey (str): name of primary (or surrogate) key
            data_to_ingest (DataFrame) : dataframe containing new/updated/deleted records

        Notes:
            Based on latest version of google-cloud library (3.15.0) (https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client)
            method update_rows does not exist anymore

        """

        sql_update_stmt = DataIngestor.UPDATE_STMT.format(
            destination_table = destination_table,
            pkey = pkey,
            list_pkeys_involved = str(set(data_to_ingest[pkey])).replace('{','').replace('}','')
        )


        self.__bigquery_manager.run_query(sql_update_stmt)
