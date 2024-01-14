from lib.data.ingestor.DataIngestor import DataIngestor
import logging
from unittest import mock
from pandas import DataFrame


class TestDataIngestor():

  ### Class setup/teardown

    @classmethod
    def setup_class (cls):
        pass

    @classmethod
    def teardown_class (cls):
        """ setup_class is invoked after class tests execution """
        pass

    ### Method setup/teardown
    def setup_method (self):
        """ setup_class is invoked before class tests execution """
        pass


    def teardown_method(self):
        """ teardown is invoked after every test method """
        pass

    ### Tests
    @mock.patch('google.cloud.bigquery.Client')
    def test_ingest_data(self, mock_bigquery):
        ingestor = DataIngestor(bigquery_connection=mock_bigquery)
        res = ingestor.ingest_data('source_table', 'dest_table', 'pkey')
        assert res == 0
