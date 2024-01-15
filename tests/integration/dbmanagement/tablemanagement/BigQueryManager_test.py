import logging, pytest
from datetime import date, datetime, time, timedelta

import pandas as pd
from google.cloud.bigquery import (Client, QueryJobConfig, SchemaField, Table, job)

from lib.dbmanagement.connector.BigQueryConnector import BigQueryConnector
from lib.dbmanagement.tablemanagement.BigQueryManager import BigQueryManager


class TestBigQueryManager():
    PROJECT_IT = 'worldline-prj'
    SAMPLE_DATASET_NAME = 'test'

    SAMPLE_TABLE_NAME = 'transaction_table'
    SAMPLE_TABLE_ID = 'test.transaction_table'

    SAMPLE_TABLE_SCHEMA = [
        SchemaField(name = 'sample_id', field_type = 'STRING', mode = 'NULLABLE' ),
        SchemaField(name = 'name', field_type = 'STRING', mode = 'NULLABLE' ),
        SchemaField(name = 'value', field_type = 'INTEGER',mode = 'NULLABLE' )
    ]

    SAMPLE_PKEY = 'sample_id'


  ### Class setup/teardown

    @classmethod
    def setup_class (cls):
        """ setup_class is invoked before class tests execution """

        cls.logger = logging.getLogger()
        cls.bigquery_client = BigQueryConnector(project_id=cls.PROJECT_IT).get_client()

        cls.bq_manager = BigQueryManager(
            bigquery_client = cls.bigquery_client
        )

        # delete source and dest table

        cls.bigquery_client.delete_dataset(f'{cls.PROJECT_IT}.{cls.SAMPLE_DATASET_NAME}',delete_contents=True, not_found_ok=True)

        test_table = Table (
            table_ref = f'{cls.PROJECT_IT}.{cls.SAMPLE_TABLE_ID}',
            schema = cls.SAMPLE_TABLE_SCHEMA
            )

        cls.bigquery_client.create_dataset(f'{cls.PROJECT_IT}.{cls.SAMPLE_DATASET_NAME}', exists_ok=True)

        #create empty table
        cls.bigquery_client.create_table(test_table)


        sample_data = pd.DataFrame({
            'sample_id': ['100', '101', '102'],
            'name': ['test', 'test1_updated', 'test2'],
            'value': [0,1,2]
        })
        insert_src = ",".join([str(el) for el in list(sample_data.itertuples(index=False, name=None))])

        cls.bigquery_client.query (
                query = f"""INSERT INTO {cls.SAMPLE_TABLE_ID}
                VALUES {insert_src}
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

    def test_run_query(self):
        query = f"select * from {TestBigQueryManager.SAMPLE_TABLE_ID} order by {TestBigQueryManager.SAMPLE_PKEY}"
        job_query = self.bq_manager.run_query(query=query)

        # assert instance
        assert(isinstance(job_query, job.QueryJob))

        # assert results

        expected_results = pd.DataFrame({
            'sample_id': ['100', '101', '102'],
            'name': ['test', 'test1_updated', 'test2'],
            'value': [0,1,2]
        })

        results = job_query.to_dataframe().sort_values(by=[TestBigQueryManager.SAMPLE_PKEY])

        assert(results.equals(expected_results))


    def test_insert_records_success(self):
        rows = pd.DataFrame({
            'sample_id': ['103'],
            'name': ['test3'],
            'value': [3]
        })
        result = self.bq_manager.insert_records(TestBigQueryManager.SAMPLE_TABLE_ID, rows)
        expected_result = 1

        # assert return
        assert(result == expected_result)

        # assert data

        query = f"select * from {TestBigQueryManager.SAMPLE_TABLE_ID} order by {TestBigQueryManager.SAMPLE_PKEY}"
        job_query = self.bq_manager.run_query(query=query)


        expected_results = pd.DataFrame({
            'sample_id': ['100', '101', '102', '103'],
            'name': ['test', 'test1_updated', 'test2', 'test3'],
            'value': [0,1,2,3]
        })

        results = job_query.to_dataframe().sort_values(by=[TestBigQueryManager.SAMPLE_PKEY])

        assert(results.equals(expected_results))

    def test_insert_records_failure_no_table(self):
        with pytest.raises(Exception):
            result = self.bq_manager.insert_records('not_existing_table', pd.DataFrame())

    def test_insert_records_failure_not_matched_schema(self):
        with pytest.raises(Exception):
            result = self.bq_manager.insert_records(TestBigQueryManager.SAMPLE_TABLE_ID, pd.DataFrame())


    def test_update_records(self):
        rows = pd.DataFrame({
            'sample_id': ['103'],
            'name': ['test3_updated'],
            'value': [3]
        })
        result = self.bq_manager.update_records(TestBigQueryManager.SAMPLE_TABLE_ID, {'name':'"test3_updated"'}, 'value=3')
        expected_result = 1

        # assert return
        assert(result == expected_result)

        # assert data

        query = f"select * from {TestBigQueryManager.SAMPLE_TABLE_ID} order by {TestBigQueryManager.SAMPLE_PKEY}"
        job_query = self.bq_manager.run_query(query=query)


        expected_results = pd.DataFrame({
            'sample_id': ['100', '101', '102', '103'],
            'name': ['test', 'test1_updated', 'test2', 'test3_updated'],
            'value': [0,1,2,3]
        })

        results = job_query.to_dataframe().sort_values(by=[TestBigQueryManager.SAMPLE_PKEY])

        assert(results.equals(expected_results))

    def test_update_records_failure_no_table(self):
        with pytest.raises(Exception):
            result = self.bq_manager.update_records('not_existing_table', {'name':'"test3_updated"'}, 'value=3')

    def test_update_records_failure_not_matched_schema(self):
        with pytest.raises(Exception):
            result = self.bq_manager.update_records(TestBigQueryManager.SAMPLE_TABLE_ID, {'name':1234}, 'value=3')