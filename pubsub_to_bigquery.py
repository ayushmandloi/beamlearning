import logging
import sys
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, WorkerOptions, \
    SetupOptions
from apache_beam import ParDo, DoFn
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
import os
from apache_beam.io import ReadFromPubSub, WriteToBigQuery, WriteToMongoDB
from apache_beam.io.textio import ReadFromText


class MyOptions(PipelineOptions):
    """This class adds value provided arguments, which is essential while creating template"""

    @classmethod
    def _add_argparse_args(cls, parser):
        """Service account path"""
        # creation of template
        parser.add_argument('--service_account_credentials_file_path', type=str)


class Pipelinecreator:

    def __init__(self):
        self.options = PipelineOptions()
        self.runtime_options = self.options.view_as(MyOptions)
        sa_file = self.runtime_options.service_account_credentials_file_path
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(sa_file)
        self.options.view_as(StandardOptions).streaming = True
        self.options.view_as(SetupOptions).save_main_session = True

        google_cloud_options = self.options.view_as(GoogleCloudOptions)
        google_cloud_options.project = "<project-name>"
        google_cloud_options.enable_streaming_engine = False


        worker_options = self.options.view_as(WorkerOptions)
        #worker_options.autoscaling_algorithm = 'NONE'
        worker_options.autoscaling_algorithm = 'THROUGHPUT_BASED'
        worker_options.machine_type = 'n1-standard-1'
        worker_options.disk_size_gb = 30


    def run(self):
        table = "<project-name>:beam_basics.from_pubsub"
        schema = "name:string,age:integer,location:string,inserted_datetime:timestamp"

        table_error = "<project-name>:beam_basics.error"
        schema_error = "data:string,error:string,inserted_datetime:timestamp"

        sub = "projects/<project-name>/subscriptions/beam_testing-sub"

        pipeline = beam.Pipeline(options=self.options)

        data_from_pubsub = pipeline | ReadFromPubSub(subscription=sub)
        valid_data, invalid_data = data_from_pubsub | ParDo(Transformation()).with_outputs("invalid_data", main='valid_data')

        valid_data | "Write To BigQuery" >> WriteToBigQuery(table=table,
                                  create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                  write_disposition=BigQueryDisposition.WRITE_APPEND)

        invalid_data | "Write To Error BigQuery" >> WriteToBigQuery(table=table_error, schema=schema_error,
                                  create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                  write_disposition=BigQueryDisposition.WRITE_APPEND)

        pipeline.run().wait_until_finish()


class Transformation(DoFn):

    def process(self, element, *args, **kwargs):

        import json
        import datetime
        print(element)
        data = json.loads(element)
        print(data)
        data["inserted_datetime"] = datetime.datetime.now()
        return [data]



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("__name__")
    Pipelinecreator().run()
