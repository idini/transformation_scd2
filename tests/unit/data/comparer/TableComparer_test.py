from lib.data.comparer.TableComparer import TableComparer
from lib.dbmanagement.tablemanagement.BigQueryManager import BigQueryManager
import logging
from pandas import DataFrame
from unittest import mock


class TestTableComparer():

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

    @mock.patch('google.cloud.bigquery.Client', autospec=True)
    @mock.patch('lib.dbmanagement.tablemanagement.BigQueryManager')
    def test_compare_tables(self, mock_bigquery, mock_BigQueryManager):

        test_data = DataFrame(
            data = {'id': [101, 102],
                    'col1': ['a', 'b'],
                    'operation' : [1, 2]
                    }
        )

        mock_BigQueryManager.run_query = lambda x : test_data

        comparer = TableComparer(bigquery_connection=mock_bigquery)
        data_to_ingest = comparer.compare_tables('source_table', 'dest_table')

        assert data_to_ingest.equals(test_data)
