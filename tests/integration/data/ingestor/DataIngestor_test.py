from datetime import datetime, date, timedelta, time
from google.cloud.bigquery import Table, SchemaField
from lib.dbmanagement.connector.BigQueryConnector import BigQueryConnector
from lib.data.ingestor.DataIngestor import DataIngestor
import pandas as pd
import logging
import pytest

class TestDataIngestor ():
    PROJECT_IT = 'my-prj'
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
        SchemaField(name = 'TechnicalKey', field_type = 'INTEGER', mode = 'REQUIRED' ),
        SchemaField(name = 'sample_id', field_type = 'STRING', mode = 'NULLABLE' ),
        SchemaField(name = 'name', field_type = 'STRING', mode = 'NULLABLE' ),
        SchemaField(name = 'value', field_type = 'INTEGER',mode = 'NULLABLE' ),
        SchemaField(name = 'Date_From', field_type = 'DATETIME', mode = 'REQUIRED' ),
        SchemaField(name = 'Date_To', field_type = 'DATETIME', mode = 'REQUIRED' ),
        SchemaField(name = 'Is_valid', field_type = 'STRING',mode = 'REQUIRED' )
    ]

    PKEY = 'sample_id'

  ### Class setup/teardown

    @classmethod
    def setup_class (cls):
        """ setup_class is invoked before class tests execution """

        cls.logger = logging.getLogger()
        cls.bigquery_client = BigQueryConnector(project_id=cls.PROJECT_IT).get_client()

        cls.ingestor = DataIngestor(
            bigquery_connection = cls.bigquery_client
        )

        # delete source and dest table

        cls.bigquery_client.delete_dataset(f'{cls.PROJECT_IT}.{cls.SAMPLE_DATASET_NAME}',delete_contents=True, not_found_ok=True)

        src_table = Table (
            table_ref = f'{cls.PROJECT_IT}.{cls.SAMPLE_SOURCE_TABLE_ID}',
            schema = cls.SAMPLE_SOURCE_TABLE_SCHEMA
            )

        dest_table = Table (
            table_ref = f'{cls.PROJECT_IT}.{cls.SAMPLE_DEST_TABLE_ID}',
            schema = cls.SAMPLE_DEST_TABLE_SCHEMA
            )

        cls.bigquery_client.create_dataset(f'{cls.PROJECT_IT}.{cls.SAMPLE_DATASET_NAME}', exists_ok=True)

        #create empty table
        cls.bigquery_client.create_table(src_table)
        cls.bigquery_client.create_table(dest_table)


        sample_source = pd.DataFrame({
            'sample_id': ['100', '101', '102'],
            'name': ['test', 'test1_updated', 'test2'],
            'value': [0,1,2]
        })
        insert_src = ",".join([str(el) for el in list(sample_source.itertuples(index=False, name=None))])

        cls.bigquery_client.query (
                query = f"""INSERT INTO {cls.SAMPLE_SOURCE_TABLE_ID}
                VALUES {insert_src}
                """
            ).result()

        sample_dest_data = pd.DataFrame({
            'TechnicalKey':[1234,5678,9012],
            'sample_id': ['100', '101', '103'],
            'name': ['test', 'test1', 'test3_deleted'],
            'value': [0,1,3],
            'Date_From': ['2022-01-01T00:00:00','2023-01-01T00:00:00','2010-01-01T00:00:00'],
            'Date_To': ['9999-01-01T00:00:00', '9999-01-01T00:00:00', '9999-01-01T00:00:00'],
            'Is_valid': ['yes','yes','yes']

        })

        insert_dest = ",".join([str(el) for el in list(sample_dest_data.itertuples(index=False, name=None))])

        cls.bigquery_client.query (
                query = f"""INSERT INTO {cls.SAMPLE_DEST_TABLE_ID}
                VALUES {insert_dest}
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

    def test_ingest_data(self):
        """ it should test ingestion success """

        result = self.ingestor.ingest_data(TestDataIngestor.SAMPLE_SOURCE_TABLE_ID, TestDataIngestor.SAMPLE_DEST_TABLE_ID, TestDataIngestor.PKEY)

        # check method result
        assert(result == 0)

        # check data
        today = datetime.combine(date.today(), time.min).strftime("%Y-%m-%d %H:%M:%S")
        yesterday = datetime.combine(date.today() - timedelta(days = 1), time.min).strftime("%Y-%m-%d %H:%M:%S")

        expected_result =pd.DataFrame({
            'sample_id': ['100', '101','101', '102','103'],
            'name': ['test', 'test1', 'test1_updated', 'test2', 'test3_deleted'],
            'value': [0,1,1,2,3],
            'Date_From': ['2022-01-01 00:00:00','2023-01-01 00:00:00',today, today,'2010-01-01 00:00:00'],
            'Date_To': ['9999-01-01 00:00:00',  yesterday, '9999-01-01 00:00:00','9999-01-01 00:00:00', yesterday],
            'Is_valid': ['yes','no','yes','yes','no']

        })
        # 101 updated, 102 inserted, 103 deleted

        query_job = self.bigquery_client.query (
                query = f"""select *
                from {TestDataIngestor.SAMPLE_DEST_TABLE_ID}"""
            ).result()
        result = query_job.to_dataframe()
        result = result.drop(columns=['TechnicalKey']).sort_values(by=['sample_id','name']).reset_index(drop=True) # tech key generated at runtime
        expected_result = expected_result.sort_values(by=['sample_id','name']).reset_index(drop=True)
        expected_result['Date_From'] = expected_result['Date_From'].astype('str').replace('T',' ')
        expected_result['Date_To'] = expected_result['Date_To'].astype('str').replace('T',' ')
        result['Date_From'] = result['Date_From'].astype('str').replace('T',' ')
        result['Date_To'] = result['Date_To'].astype('str').replace('T',' ')

        assert result.equals(expected_result)

