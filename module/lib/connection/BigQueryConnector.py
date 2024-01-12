from google.cloud import bigquery

class BigQueryConnector():
    """
        Class lass that initialize bigquery client object based on specific project id.

        Args:
            project_id (str): id of Bigquery project.

    """

    def __init__(self, project_id) -> None:
        self.__project_id = project_id

        # Create a BigQuery client
        self.__client = bigquery.Client(project=self.__project_id)

    def get_client(self):
        """ method that returns bigquery client initialized in constructor

        Returns:
            Client: the Client object from bigquery

        """
        return self.__client
