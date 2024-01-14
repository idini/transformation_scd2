import pandas
from datetime import datetime
from google.cloud.bigquery import Table, SchemaField
from lib.dbmanagement.connector.BigQueryConnector import BigQueryConnector
from lib.data.comparer.TableComparer import TableComparer
import pandas as pd
import logging
import pytest

class TestTableComparer ():
    PROJECT_IT = 'worldline-prj'
    SAMPLE_DATASET_NAME = 'test'
    SAMPLE_SOURCE_TABLE_NAME = 'source_transaction_table'
    SAMPLE_SOURCE_TABLE_ID = 'test.source_transaction_table'

    SAMPLE_SOURCE_TABLE_SCHEMA = [
        SchemaField(name = 'sample_id', field_type = 'STRING', mode = 'NULLABLE' ),
        SchemaField(name = 'name', field_type = 'STRING', mode = 'NULLABLE' ),
        SchemaField(name = 'value', field_type = 'INTEGER',mode = 'NULLABLE' )
    ]

    SAMPLE_DEST_TABLE_NAME = 'dest_transaction_table'
    SAMPLE_DEST_TABLE_ID = 'test.dest_transaction_table'

    SAMPLE_DEST_TABLE_SCHEMA = [
        SchemaField(name = 'TechnicalKey', field_type = 'STRING', mode = 'REQUIRED' ),
        SchemaField(name = 'sample_id', field_type = 'STRING', mode = 'NULLABLE' ),
        SchemaField(name = 'name', field_type = 'STRING', mode = 'NULLABLE' ),
        SchemaField(name = 'value', field_type = 'INTEGER',mode = 'NULLABLE' ),
        SchemaField(name = 'Date_From', field_type = 'DATETIME', mode = 'REQUIRED' ),
        SchemaField(name = 'Date_To', field_type = 'DATETIME', mode = 'REQUIRED' ),
        SchemaField(name = 'Is_valid', field_type = 'STRING',mode = 'REQUIRED' )
    ]

  ### Class setup/teardown

    @classmethod
    def setup_class (cls):
        """ setup_class is invoked before class tests execution """

        cls.logger = logging.getLogger()
        cls.bigquery_client = BigQueryConnector(project_id=cls.PROJECT_IT).get_client()

        cls.comparer = TableComparer(
            bigquery_connection = cls.bigquery_client
        )

        # delete source and dest table
        cls.bigquery_client.delete_table(
            table = f'{cls.SAMPLE_DATASET_NAME}.{cls.SAMPLE_SOURCE_TABLE_NAME}',
            not_found_ok = True
        )
        cls.bigquery_client.delete_table(
            table = f'{cls.SAMPLE_DATASET_NAME}.{cls.SAMPLE_DEST_TABLE_NAME}',
            not_found_ok = True
        )


        src_table = Table (
            table_ref = f'{cls.PROJECT_IT}.{cls.SAMPLE_SOURCE_TABLE_ID}',
            schema = TestTableComparer.SAMPLE_SOURCE_TABLE_SCHEMA
            )

        dest_table = Table (
            table_ref = f'{cls.PROJECT_IT}.{cls.SAMPLE_DEST_TABLE_ID}',
            schema = TestTableComparer.SAMPLE_DEST_TABLE_SCHEMA
            )

        cls.bigquery_client.create_dataset(f'{cls.PROJECT_IT}.{cls.SAMPLE_DATASET_NAME}', exists_ok=True)

        #create empty table
        cls.bigquery_client.create_table(src_table)
        cls.bigquery_client.create_table(dest_table)

        sample_data = {
            'sample_id': '100', 'name': 'test1', 'value': 1
        }

        cls.bigquery_client.query (
                query = f"""INSERT INTO {cls.SAMPLE_SOURCE_TABLE_ID}
                VALUES {tuple(sample_data.values())}
                """
            ).result()

    @classmethod
    def teardown_class (cls):
        """ setup_class is invoked after class tests execution """
        cls.bigquery_client.delete_dataset(f'{cls.PROJECT_IT}.{cls.SAMPLE_DATASET_NAME}',delete_contents=True, not_found_ok=True)

    ### Method setup/teardown

    def setup_method (self):
        """  setup_method is invoked before every test method """
        pass

    def teardown_method (self):
        """ teardown is invoked after every test method """
        pass

    ### Tests

    def test_compare_tables(self):
        """ it should test transaction success """

        result = self.comparer.compare_tables(TestTableComparer.SAMPLE_SOURCE_TABLE_ID, TestTableComparer.SAMPLE_DEST_TABLE_ID)

        expected_result = pd.DataFrame(data={'sample_id': ['100'], 'name': ['test1'], 'value': [1], 'operation': 1})

        assert result.equals(expected_result)

    def test_compare_tables_raise_error(self):
        """ it should test transaction failure """
        with pytest.raises(Exception):
            result = self.comparer.compare_tables(TestTableComparer.SAMPLE_SOURCE_TABLE_ID, 'not_existing_table')