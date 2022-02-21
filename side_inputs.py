import logging
import sys
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, WorkerOptions, \
    SetupOptions
from apache_beam import ParDo, DoFn

from apache_beam.io.fileio import ReadMatches, MatchAll

from apache_beam.io.gcp import gcsfilesystem
from apache_beam.io import ReadFromBigQuery
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.gcp import gcsfilesystem
import os
from apache_beam.io.textio import ReadFromText, WriteToText
from apache_beam.io import ReadFromPubSub, WriteToBigQuery, WriteToMongoDB

class MyOptions(PipelineOptions):
    """This class adds value provided arguments, which is essential while creating template"""

    @classmethod
    def _add_argparse_args(cls, parser):
        """Service account path"""
        # creation of template
        parser.add_argument('--service_account_credentials_file_path', type=str)

        # while running our template
        parser.add_value_provider_argument('--gcp_folder', type=str)
        parser.add_value_provider_argument('--side_input', type=str)


class Pipelinecreator:

    def __init__(self):
        self.options = PipelineOptions()
        self.runtime_options = self.options.view_as(MyOptions)
        sa_file = self.runtime_options.service_account_credentials_file_path
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(sa_file)
        self.options.view_as(StandardOptions).streaming = False
        self.options.view_as(SetupOptions).save_main_session = True

        google_cloud_options = self.options.view_as(GoogleCloudOptions)
        google_cloud_options.project = <project-name-gcp>
        google_cloud_options.enable_streaming_engine = False


        worker_options = self.options.view_as(WorkerOptions)
        #worker_options.autoscaling_algorithm = 'NONE'
        worker_options.autoscaling_algorithm = 'THROUGHPUT_BASED'
        worker_options.machine_type = 'n1-standard-1'
        worker_options.disk_size_gb = 30

    def run(self):
        pipeline = beam.Pipeline(options=self.options)

        side_input = pipeline | "Read side input file" >> ReadFromText(self.runtime_options.side_input)
        # username.csv
        # self.runtime_options.side_input

        output_main_file  = pipeline | "Read main input file" >> ReadFromText(self.runtime_options.gcp_folder, skip_header_lines=1)
        # username-password-recovery-code.csv
        # self.runtime_options.gcp_folder
        valid_data, invalid_data = output_main_file | ParDo(Transformation(), side_data = beam.pvalue.AsList(side_input)).with_outputs("invalid_data", main='valid_data')


        pipeline.run().wait_until_finish()


class Transformation(DoFn):
    def process(self, element, side_data):

        logging.info("element " + element)
        header_of_element = "Username; Identifier;One-time password;Recovery code;First name;Last name;Department;Location"
        data = {i:j for i,j in zip(header_of_element.split(";"),element.split(";"))}
        # print("data with header " + str(data))
        # print("side data ")
        # print(side_data)


        header = side_data[0].split(";")

        list_data = []

        for i in side_data[1:]:
            side_dict_data= {}
            if i:
                new_i = i.split(";")
                for j,k in enumerate(new_i):
                    side_dict_data[header[j]]=k

                list_data.append(side_dict_data)


        logging.info(list_data)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("__name__")
    Pipelinecreator().run()

#gcloud dataflow jobs run beam-learb --gcs-location gs://<bucket-name>/templates/beamlearning --parameters gcp_folder=gs://test-global-data/username-password-recovery-code.csv, side_input=gs://<bucket-name>/username.csv
