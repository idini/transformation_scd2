from lib.connection.BigQueryConnector import BigQueryConnector
from lib.tablemanagement.BigQueryManager import BigQueryManager
from lib.data.comparer.TableComparer import TableComparer
from lib.data.ingestor.DataIngestor import DataIngestor


if __name__ == "__main__":

    project_id = 'worldline-prj'
    src_table  = 'transformation_scd2.table_1_partners_input'
    dest_table = 'transformation_scd2.table_2_partners_output'
    pkey       = 'PartnerID'

    bq_client = BigQueryConnector(project_id).get_client()

    bq_manager = BigQueryManager(bq_client)

    comparer = TableComparer(bq_manager)

    ingestor = DataIngestor(bq_manager)

    data_to_ingest = comparer.compare_tables(src_table, dest_table)

    ingestor.ingest_data(dest_table, pkey, data_to_ingest)