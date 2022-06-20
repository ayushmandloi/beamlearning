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

        # while running our template
        parser.add_value_provider_argument('--gcp_folder', type=str)


class Pipelinecreator:

    def __init__(self):
        self.options = PipelineOptions()
        self.runtime_options = self.options.view_as(MyOptions)
        sa_file = self.runtime_options.service_account_credentials_file_path
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(sa_file)
        self.options.view_as(StandardOptions).streaming = False
        self.options.view_as(SetupOptions).save_main_session = True

        google_cloud_options = self.options.view_as(GoogleCloudOptions)
        google_cloud_options.project = "<project>"
        google_cloud_options.enable_streaming_engine = False


        worker_options = self.options.view_as(WorkerOptions)
        #worker_options.autoscaling_algorithm = 'NONE'
        worker_options.autoscaling_algorithm = 'THROUGHPUT_BASED'
        worker_options.machine_type = 'n1-standard-1'
        worker_options.disk_size_gb = 30

    def run(self):
        table = "<project>:beam_basics.from_csv"
        schema = "username:string,identifier:string,one_time_password:string,recovery_code:string,first_name:string,last_name:string,department:string,location:string"

        table_error = "<project>:beam_basics.error"
        schema_error = "data:string,error:string,inserted_datetime:timestamp"

        pipeline = beam.Pipeline(options=self.options)

        data_from_csv = pipeline | ReadFromText(self.runtime_options.gcp_folder, skip_header_lines=1)
        valid_data, invalid_data = data_from_csv | ParDo(Transformation()).with_outputs("invalid_data", main='valid_data')

        valid_data | "Write To BigQuery" >> WriteToBigQuery(table=table, schema=schema,
                                  create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                  write_disposition=BigQueryDisposition.WRITE_APPEND)

        invalid_data | "Write To Error BigQuery" >> WriteToBigQuery(table=table_error, schema=schema_error,
                                  create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                  write_disposition=BigQueryDisposition.WRITE_APPEND)

        pipeline.run().wait_until_finish()

class Transformation(DoFn):


    def process(self, element, *args, **kwargs):
        logging.info("element " + element)
        header_of_element = "username; identifier;one_time_password;recovery_code;first_name;last_name;department;location"
        from apache_beam import pvalue
        error_dict = {}
        import datetime

        try:
            data = {i.strip(): j for i, j in zip(header_of_element.split(";"), element.split(";"))}
            if len(data) == 8:
                logging.info("data sent")
                yield data
            else:
                logging.error("length of column is improper = " + str(len(data)))
                error_dict["data"] = str(data)
                error_dict["error"] = "improper length of data"
                error_dict["inserted_datetime"] = datetime.datetime.now()
                yield pvalue.TaggedOutput("invalid_data", error_dict)
        except Exception as e:
            logging.error(str(e))
            error_dict["data"] = element
            error_dict["error"] = str(e)
            error_dict["inserted_datetime"] = datetime.datetime.now()

            yield pvalue.TaggedOutput("invalid_data", error_dict)



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("__name__")
    Pipelinecreator().run()



