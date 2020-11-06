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
with fs.open('gs://teste-dotz-picarelli/csv/comp_boss.csv') as f:
    df = pd.read_csv(f)
    print(df)
df.to_csv('gs://teste-dotz-picarelli/csv/comp_boss_output.csv',index=False, encoding='utf8')


class DataIngestion:

# este método analisa o csv de entrada e converte em um dicionário que pode ser salvo pelo BigQuery
    def parse_method(self, string_input):
        values = re.split(",",
                          re.sub('\r\n', '', re.sub(u'"', '', string_input)))
        row = dict(
            zip(('component_id','component_type_id','type','connection_type_id','outside_shape','base_type','height_over_tube','bolt_pattern_long','bolt_pattern_wide','groove','base_diameter','shoulder_diameter','unique_feature','orientation','weight'),
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
        default='gs://teste-dotz-picarelli/csv/comp_boss_output.csv')

    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='dotz_data.comp_boss')

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
             known_args.output,

             schema='component_id:STRING,component_type_id:STRING,type:STRING,connection_type_id:STRING,outside_shape:STRING,base_type:STRING,height_over_tube:FLOAT,bolt_pattern_long:FLOAT,bolt_pattern_wide:FLOAT,groove:STRING,base_diameter:FLOAT,shoulder_diameter:FLOAT,unique_feature:STRING,orientation:STRING,weight:FLOAT',

             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
    
