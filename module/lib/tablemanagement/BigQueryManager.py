from google.cloud.bigquery import Client, Table
import logging
from pandas import DataFrame


class BigQueryManager():
    """
    Class that implements CRUD ops on Google Bigquery tables.

    Args:
        bigquery_client (Client): client of Bigquery.

    """

    def __init__(
            self,
            bigquery_client:Client
        ) -> None:

        self.__client = bigquery_client
        self.__logger = logging.getLogger()

    def create_dataset(self, dataset_name:str)->None:
        """ method used to create a dataset on bigquery

        Args:
            dataset_name (str): name of dataset

        Examples:
            >>> bq_manager = BigqueryManager(client)
            >>> bq_manager.create_dataset(dataset_name = 'test')

        """
        # initialize dataset reference
        dataset_ref = self.__client.dataset(dataset_name)

        # set location for new dataset
        dataset_ref.location = "europe-west6"

        # create dataset
        self.__client.create_dataset(dataset_ref, timeout=30, exists_ok= True)
        self.__logger.info(f'Dataset {dataset_name} created.')

    def create_table(self, dataset_name:str, table_name:str, schema:list = None) -> None:
        """ method used to create a table on bigquery

        Args:
            dataset_name (str): name of dataset
            table_name (str): name of table
            schema (list, optional): list of SchemaField objects

        Examples:
            >>> bq_manager = BigqueryManager(client)
            >>> bq_manager.create_table(dataset_name = 'test_dataset', table_name = 'test_table)

        Notes:
            If dataset does not exists, the method creates it before table creation.
            Furthermore, the table can be created without a schema definition

        """
        # check if dataset exists, if not, create it
        self.create_dataset(dataset_name=dataset_name)

        # initialize table reference
        table_ref = self.__client.dataset(dataset_name).table(table_name)

        table_config = Table(table_ref, schema=schema)
        self.__client.create_table(table_config, exists_ok=True)
        self.__logger.info(f'Table {dataset_name}.{table_name} created.')

    def run_query(self, query:str) -> DataFrame:
        """ method that runs a sql query on bigquery

        Args:
            query (str): query statement

        Returns:
            pandas.DataFrame: pandas DataFrame contains the results of query

        """

        query_job = self.__client.query(query, location=self.__client.location)

        # Wait for the query to complete
        query_job.result()

        # Get the results
        return query_job.to_dataframe()

    def insert_data(self, destination_table:str, rows:DataFrame, chunk_size = 1000) -> None:
        """ method that insert records to a table from a pandas dataframe

        Args:
            destination_table (str): name of table
            rows (DataFrame): pandas.DataFrame containing the new records
            chunk_size (int, optional) : chunk size


        Notes:
            rows should respect the schema of destination table

        """


        table_ref = self.__client.get_table(destination_table)
        res = self.__client.insert_rows_from_dataframe(
            table = table_ref,
            dataframe = rows,
            chunk_size = chunk_size
        )

        if True in [ True for el in res if len(el) > 0]:
            self.__logger.error("Failed to insert rows.")
            raise Exception("Failed to insert rows.")


    def update_data(self, destination_table:str, assignments:dict, filter_cond:str) -> None:
        """ method that insert records to a table from a pandas dataframe

        Args:
            destination_table (str): name of table
            assignments (dict): dict containing the couple column -> value
            filter_cond (str) : filter statement


        Notes:
            column names should exists in the destination table
        """

        update_stmt = """
                update `{destination_table}`
                set {assignments}
                where {filter_cond};
            """.format(
                destination_table = destination_table,
                assignments = ",".join([str(column) + "=" + str(value) for column, value in assignments.items()]),
                filter_cond = filter_cond
            )

        self.__logger.debug("update stmt -> " + update_stmt)

        self.run_query(update_stmt)

