import argparse
import logging

import apache_beam as beam
import apache_beam.io.gcp.bigquery as bq

SCHEMA = 'ID:INTEGER,CLASS:STRING,SALES:FLOAT'
table_id = 'my-bq-demo:output.out_table'

class DataIngestion:
    """A helper class which contains the logic to translate the file into
    a format BigQuery will accept."""

    def parse_method(self, string_input):
        # Strip out carriage return, newline and quote characters.
        values = string_input.split(",")
        row = dict(
            zip(('ID', 'CLASS', 'SALES'),
                values))
        return row


def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()

    parser.add_argument('--input')
    # parser.add_argument('--output')

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    # DataIngestion is a class we built in this script to hold the logic for
    # transforming the file into a BigQuery table.
    data_ingestion = DataIngestion()

    with beam.Pipeline(argv=pipeline_args) as p:
        result = ( 
                    p
                    | 'Read from a File' >> beam.io.ReadFromText(known_args.input,
                                                                skip_header_lines=1)
                    | 'String To BigQuery Row' >>
                    beam.Map(lambda s: data_ingestion.parse_method(s)) 
                    | 'Write to BigQuery' >> bq.WriteToBigQuery(
                            table = table_id, schema = SCHEMA,
                            create_disposition = bq.BigQueryDisposition.CREATE_IF_NEEDED,
                            write_disposition=bq.BigQueryDisposition.WRITE_APPEND)
                    )
    # result = p.run()
    # result.wait_until_finish()



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.WARNING)
    run()