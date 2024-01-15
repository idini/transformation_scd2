import logging

from google.cloud.bigquery import Client

from lib.dbmanagement.connector.BigQueryConnector import BigQueryConnector


class TestBigQueryManager():
    PROJECT_IT = 'worldline-prj'

  ### Class setup/teardown

    @classmethod
    def setup_class (cls):
        """ setup_class is invoked before class tests execution """

        cls.logger = logging.getLogger()
        cls.bq_connector = BigQueryConnector(project_id=cls.PROJECT_IT)


    @classmethod
    def teardown_class (cls):
        """ setup_class is invoked after class tests execution """
        pass

    ### Method setup/teardown

    def setup_method (self):
        """  setup_method is invoked before every test method """
        pass

    def teardown_method (self):
        """ teardown is invoked after every test method """
        pass

    ### Tests

    def test_get_client(self):
        client = self.bq_connector.get_client()
        assert(isinstance(client, Client))
