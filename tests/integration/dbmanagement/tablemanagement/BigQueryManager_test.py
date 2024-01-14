# import pandas
# from datetime import datetime
# from google.cloud.bigquery import Table, SchemaField
# from lib.dbmanagement.connector.BigQueryConnector import BigQueryConnector
# from lib.dbmanagement.transaction.BigqueryTransaction import BigqueryTransaction
# import pandas as pd
# import logging

# class TestTransaction ():
#     PROJECT_IT = 'worldline-prj'
#     SAMPLE_DATASET_NAME = 'test'
#     SAMPLE_TABLE_NAME = 'transaction_table'
#     SAMPLE_TABLE_ID = 'test.transaction_table'

#     SAMPLE_TABLE_SCHEMA = [
#         SchemaField(name = 'sample_id', field_type = 'STRING', mode = 'NULLABLE' ),
#         SchemaField(name = 'name', field_type = 'STRING', mode = 'NULLABLE' ),
#         SchemaField(name = 'value', field_type = 'INTEGER',mode = 'NULLABLE' ),
#         SchemaField(name = 'createdAt', field_type = 'DATETIME', mode = 'NULLABLE' )
#     ]

#   ### Class setup/teardown

#     @classmethod
#     def setup_class (cls):
#         """ setup_class is invoked before class tests execution """

#         cls.logger = logging.getLogger()
#         cls.bigquery_client = BigQueryConnector(project_id=cls.PROJECT_IT).get_client()

#         cls.bigquery_transaction = BigqueryTransaction(
#             bigquery_connection = cls.bigquery_client
#         )

#         # delete transaction table
#         cls.bigquery_client.delete_table(
#             table = f'{cls.SAMPLE_DATASET_NAME}.{cls.SAMPLE_TABLE_NAME}',
#             not_found_ok = True
#         )


#         table = Table (
#             table_ref = f'{cls.PROJECT_IT}.{cls.SAMPLE_TABLE_ID}',
#             schema = TestTransaction.SAMPLE_TABLE_SCHEMA
#             )

#         cls.bigquery_client.create_dataset(f'{cls.PROJECT_IT}.{cls.SAMPLE_DATASET_NAME}', exists_ok=True)

#         #create empty table
#         cls.bigquery_client.create_table(table)

#     @classmethod
#     def teardown_class (cls):
#         """ setup_class is invoked after class tests execution """
#         cls.bigquery_client.delete_dataset(f'{cls.PROJECT_IT}.{cls.SAMPLE_DATASET_NAME}',delete_contents=True, not_found_ok=True)

#     ### Method setup/teardown

#     def setup_method (self):
#         """  setup_method is invoked before every test method """
#         pass

#     def teardown_method (self):
#         """ teardown is invoked after every test method """
#         pass

#     ### Tests

#     def test_commit_transaction (self):
#         """ it should test transaction success """
#         timestamp = datetime(2024, 1, 1, 1, 2, 3)

#         sample_data = {
#             'sample_id': '100', 'name': 'test1', 'value': 1, 'createdAt': f'{str(timestamp)}'
#         }

#         try:
#             job_config = self.bigquery_transaction.begin_transaction()

#             self.bigquery_client.query (
#                 query = f"""INSERT INTO {self.SAMPLE_TABLE_ID}
#                 VALUES {tuple(sample_data.values())}
#                 """,
#                 job_config = job_config
#             ).result()
#             self.bigquery_transaction.commit_transaction()
#         except Exception as error:
#             print(error)
#             self.bigquery_transaction.rollback_transaction()
#             assert False

#         # read target
#         result = self.bigquery_client.query(
#             f'SELECT * FROM {self.SAMPLE_TABLE_ID}',
#             job_config = job_config
#         ).to_dataframe()
#         expected_result = pd.DataFrame(sample_data)

#         print("RESULTS "+str(result))
#         print("EXPECTED "+str(expected_result))
#         assert result.equals(expected_result)


#     def test_rollback_transaction (self):
#         """ it should test transaction rollback """

#         timestamp = datetime(2022, 12, 13, 11, 12, 13, 123456)
#         sample_data = {
#             'sample_id': '100', 'name': 'test1', 'value': 1, 'createdAt': f'{str(timestamp)}'
#         }

#         try:
#             job_config = self.bigquery_transaction.begin_transaction()


#             self.bigquery_client.query (
#                 query = f"""
#                 DELETE FROM {self.SAMPLE_TABLE_ID}
#                 WHERE 'sample_id' = '100'
#                 """,
#                 job_config = job_config
#             ).result()

#             self.bigquery_client.query (
#                 query = """ERROR('Intentional raising of error for Failure of transaction');""",
#                 job_config = job_config
#             ).result()

#             self.bigquery_transaction.commit_transaction()
#             assert False
#         except Exception as error: #rollback
#             self.bigquery_transaction.rollback_transaction()

#         # read target
#         result = self.bigquery_client.query(
#             f'SELECT * FROM {self.SAMPLE_TABLE_ID}',
#             job_config = job_config
#         ).to_dataframe()
#         expected_result = pd.DataFrame(sample_data)

#         assert result.equals(expected_result)