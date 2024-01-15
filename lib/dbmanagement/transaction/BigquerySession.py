from google.cloud.bigquery.query import ConnectionProperty
from google.cloud.bigquery import Client, QueryJobConfig, LoadJobConfig

class BigquerySession (object):
    """ContextManager wrapping a bigquerySession."""

    def __init__(
        self,
        bigquery_client:Client
    ) -> None:
        """Construct instance."""
        self.__client = bigquery_client
        self.__session_id:str = None
        self.__session_job:QueryJobConfig = None

    def start_session (self):
        """Initiate a Bigquery session and return the session_id."""
        res = self.__client.query(
            "SELECT 3;",  # a query can't fail
            job_config = QueryJobConfig(
                create_session = True,
                use_legacy_sql = False,
                write_disposition="WRITE_APPEND",
                ),
        )
        res.result()  # wait job completion

        self.__session_id = res.session_info.session_id
        self.__session_job = QueryJobConfig(
            create_session=False,
            use_legacy_sql = False,
            connection_properties=[
                ConnectionProperty(
                    key="session_id", value=self.__session_id

                )
            ],
        )

        return self.__session_job

    def __enter__ (self) -> str:
        self.start_session()
        return self.__session_id

    def get_session_id (self):
        return self.__session_id


    def end_session (self):
        if self.__session_id:
            # abort the session in any case to have a clean state at the end
            # (sometimes in case of script failure, the table is locked in
            # the session)
            job = self.__client.query(
                "CALL BQ.ABORT_SESSION();",
                self.__session_job,
            )
            job.result()

    def __exit__ (self, exc_type, exc_value, traceback):
        """Abort the opened session."""
        self.end_session()
