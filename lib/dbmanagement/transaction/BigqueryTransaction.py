"""ContextManager wrapping a bigquery session."""
import logging
from logging import Logger
from google.cloud.bigquery import Client
from lib.dbmanagement.transaction.BigquerySession import BigquerySession


class BigqueryTransaction (): #TODO this class should contain the logic of BigQuerySession

    def __init__(
        self,
        bigquery_connection
    ) -> None:
        """"""
        self.__bigquery_connection:Client = bigquery_connection
        self.__job_config = None
        self.__logger = logging.getLogger()
        self.__bigquery_session:BigquerySession = None

    def begin_transaction (self):
        self.__bigquery_session = BigquerySession(bigquery_client = self.__bigquery_connection)
        self.__job_config = self.__bigquery_session.start_session()

        self.__logger.warn(str(self.__job_config))
        self.__logger.warn(self.__bigquery_session.get_session_id())

        self.__bigquery_connection.query(
            "BEGIN TRANSACTION;",
            job_config = self.__job_config,
        ).result()

        return self.__job_config


    def commit_transaction (self):

        # throws Exception if transaction not initialized
        self.__check_existing_job()

        job = self.__bigquery_connection.query(
            "COMMIT TRANSACTION;",
            job_config = self.__job_config,
        )

        result = job.result()

        self.__bigquery_session.end_session()
        self.__job_config = None

        return result


    def rollback_transaction (self):

        # throws Exception if transaction not initialized
        self.__check_existing_job()

        job = self.__bigquery_connection.query(
            "ROLLBACK TRANSACTION;",
            job_config=self.__job_config,
        )

        result = job.result()

        self.__bigquery_session.end_session()
        self.__job_config = None

        return result

    @property
    def get_job_config (self):
        return self.__job_config

    def __check_existing_job (self):
        if not self.get_job_config:
            raise Exception('Begin transaction not initialized')
        else:
            pass

    #TODO check if needed
    # def __del__(self):
    #     # close bigquery client
    #     self.__bigquery_client.close()