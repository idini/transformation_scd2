from lib.dbmanagement.connector.BigQueryConnector import BigQueryConnector
from lib.dbmanagement.tablemanagement.BigQueryManager import BigQueryManager
from lib.data.comparer.TableComparer import TableComparer
from lib.data.ingestor.DataIngestor import DataIngestor
from flask import Flask, request
from flask_restx import Resource, Api, fields

project_id = 'my-prj'

bq_client = BigQueryConnector(project_id).get_client()
ingestor = DataIngestor(bq_client)
app = Flask(__name__)

api = Api()
api.init_app(
    app,
    title = 'Data Engineer Transformation SCD2',
    description = 'This is a simple REST API containing one endpoint. This API allows you to trigger Transformation SCD2 on bigquery project my-prj',
    version = '1.0.0',
    validate=False
)


post_request = api.model('post_request', {
    'src_table': fields.String(required=True, example='transformation_scd2.table_1_partners_input'),
    'dest_table': fields.String(required=True, example='transformation_scd2.table_2_partners_output'),
    'pkey': fields.String(required=True, example='PartnerID')
})


@api.route('/trigger/')
class TriggerTransformation(Resource):

    @api.doc(
            summary = 'Trigger Transformation SCD2 on Bigquery "my-prj" project for source and destination tables specified in parameters',
            responses={
                200: 'successful operation',
                405: 'Invalid input'
            }
    )
    @api.expect(post_request)
    def post(self):
        src_table = api.payload['src_table']
        dest_table = api.payload['dest_table']
        pkey = api.payload['pkey']

        result = ingestor.ingest_data(src_table, dest_table, pkey)

        if result:
            return 405, 'Invalid input'
        return result


if __name__ == "__main__":
    app.run(debug=True)