from __future__ import absolute_import
import argparse
import pandas as pd
import gcsfs
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os

fs = gcsfs.GCSFileSystem(project='stellar-acre-294711')
with fs.open('gs://teste-dotz-picarelli/csv/bill_of_materials.csv') as f:
    df = pd.read_csv(f)
    print(df)
df.to_csv('gs://teste-dotz-picarelli/csv/bill_of_materials_output.csv',index=False, encoding='utf8')


class DataIngestion:

# este método analisa o csv de entrada e converte em um dicionário que pode ser salvo pelo BigQuery
    def parse_method(self, string_input):
        values = re.split(",",
                          re.sub('\r\n', '', re.sub(u'"', '', string_input)))
        row = dict(
            zip(('tube_assembly_id','component_id_1','quantity_1','component_id_2','quantity_2','component_id_3','quantity_3','component_id_4','quantity_4','component_id_5','quantity_5','component_id_6','quantity_6','component_id_7','quantity_7','component_id_8','quantity_8'),
                values))
        return row
    
def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--service_account_email',
        default='dotz-service@stellar-acre-294711.iam.gserviceaccount.com'
    )

    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to read. This can be a local file or '
        'a file in a Google Storage Bucket.',
        default='gs://teste-dotz-picarelli/csv/bill_of_materials_output.csv')

    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='dotz_data.bill_of_materials')

    # Analisa os argumentos da linha de comando.
    known_args, pipeline_args = parser.parse_known_args(argv)

    data_ingestion = DataIngestion()
    
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (
     p | 'Read File from GCS' >> beam.io.ReadFromText(known_args.input,
                                                  skip_header_lines=1)    
    
     | 'String To BigQuery Row' >>
     beam.Map(lambda s: data_ingestion.parse_method(s))
     | 'Write to BigQuery' >> beam.io.Write(
         beam.io.BigQuerySink(
             # The table name is a required argument for the BigQuery sink.
             # In this case we use the value passed in from the command line.
             known_args.output,

             schema='tube_assembly_id:STRING,component_id_1:STRING,quantity_1:FLOAT,component_id_2:STRING,quantity_2:FLOAT,component_id_3:STRING,quantity_3:FLOAT,component_id_4:STRING,quantity_4:FLOAT,component_id_5:STRING,quantity_5:FLOAT,component_id_6:STRING,quantity_6:FLOAT,component_id_7:STRING,quantity_7:FLOAT,component_id_8:STRING,quantity_8:FLOAT',

             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
