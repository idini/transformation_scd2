from google.cloud.bigquery import Client, Table, QueryJobConfig, job, LoadJobConfig
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


    def run_query(self, query:str, job_config:QueryJobConfig = None) -> job.QueryJob:
        """ method that runs a sql query on bigquery

        Args:
            query (str): query statement

        Returns:
            pandas.DataFrame: pandas DataFrame contains the results of query

        """

        query_job = self.__client.query(query, location=self.__client.location, job_config=job_config)

        # Wait for the query to complete
        query_job.result()

        return query_job


    def insert_records_from_dataframe(self, destination_table:str, rows:DataFrame, job_config:LoadJobConfig = None) -> int:
        """ method that insert records to a table from a pandas dataframe using LoadJobConfig

        Args:
            destination_table (str): name of table
            rows (DataFrame): pandas.DataFrame containing the new records
            job_config (LoadJobConfig, optional): query job used for transaction. If None, method runs without transaction

        Returns:
            int: number of records inserted

        Notes:
            rows should respect the schema of destination table

        """


        try:
            job = self.__client.load_table_from_dataframe(dataframe = rows, destination = destination_table, job_config = job_config)
            job.result()
            self.__logger.info(f"Inserted table {destination_table} with {job.output_rows} rows.")
            return job.output_rows
        except Exception as error:
            self.__logger.error(f'Error loading data in table {destination_table} with error {str(error)}')
            raise error

    def insert_records(self, destination_table:str, rows:DataFrame, job_config:QueryJobConfig = None) -> None:
        """ method that insert records to a table from a pandas dataframe

        Args:
            destination_table (str): name of table
            assignments (dict): dict containing the couple column -> value
            filter_cond (str) : filter statement
            job_config (QueryJobConfig, optional): query job used for transaction. If None, method runs without transaction


        Notes:
            dataframe 'rows' should respect table schema
        """

        insert_stmt = """
                insert into `{destination_table}`
                values {assignments};
            """.format(
                destination_table = destination_table,
                assignments = ",".join([str(el) for el in list(rows.itertuples(index=False, name=None))])
            )

        self.__logger.debug("insert stmt -> " + insert_stmt)
        try:
            query_job = self.run_query(insert_stmt, job_config)
            inserted_rows = query_job.num_dml_affected_rows
            self.__logger.info(f"DML query inserts {inserted_rows} rows from {destination_table}.")
            return inserted_rows
        except Exception as e:
            self.__logger.error(f'Error inserting data in table {destination_table} with error {str(e)}')
            raise e


    def update_records(self, destination_table:str, assignments:dict, filter_cond:str, job_config = None) -> None:
        """ method that insert records to a table from a pandas dataframe

        Args:
            destination_table (str): name of table
            assignments (dict): dict containing the couple column -> value
            filter_cond (str) : filter statement
            job_config (QueryJobConfig, optional): query job used for transaction. If None, method runs without transaction


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
        try:
            query_job = self.run_query(update_stmt, job_config)
            affected_rows = query_job.num_dml_affected_rows
            self.__logger.info(f"DML query updates {affected_rows} rows from {destination_table}.")
            return affected_rows
        except Exception as e:
            self.__logger.error(f'Error updating data in table {destination_table} with error {str(e)}')
            raise e



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