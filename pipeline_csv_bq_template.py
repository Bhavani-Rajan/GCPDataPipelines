import logging
import argparse
from datetime import datetime
import requests

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.io.gcp.bigquery as bq

table_id = 'my-bq-demo:output.data_grpby_country'
SCHEMA = 'country:STRING,time_duration:FLOAT'

class CsvBqOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_value_provider_argument(
          '--input',
          help='Path of the file to read from gs://bucket_name/path_to_file')

      parser.add_value_provider_argument(
          '--output',
          help='Output file to write results to project:dataset.table.')

def parse_lines(element):
    return element.split(",")

class CalcVisitDuration(beam.DoFn):
    def process(self, element):
        dt_format ="%Y-%m-%dT%H:%M:%S"
        start_dt = datetime.strptime(element[1], dt_format)
        end_dt = datetime.strptime(element[2],dt_format)

        diff = end_dt - start_dt
        rt_elem = [element[0], diff.total_seconds()]

        yield rt_elem

class GetIPCountryOrigin(beam.DoFn):
    def process(self, element):
        ip = element[0]
        response = requests.get(f"http://ip-api.com/json/{ip}?fields=country")
        country = response.json()["country"]
        rt_elem = [ip, country]

        yield rt_elem

def map_country_to_ip(element, ip_map):
    ip = element[0]
    country = ip_map[ip]
    rt_elem = [country,element[1]]

    return rt_elem

def wrap_as_dict(element):
        # Strip out carriage return, newline and quote characters. 
        row = dict(
            zip( ('country','time_duration'),
                (element)  
                ))
        return row


def run(argv=None):

    pipeline_options = PipelineOptions()

    # parser = argparse.ArgumentParser()
    #  # Use add_value_provider_argument for arguments to be templatable
    # parser.add_argument("--input")
    # args, beam_args = parser.parse_known_args(argv)

    # with beam.Pipeline(argv=beam_args) as p:

    with beam.Pipeline(options=pipeline_options) as p:
        my_options = pipeline_options.view_as(CsvBqOptions)
        lines = ( p
                | "Read File" >> beam.io.ReadFromText(my_options.input, skip_header_lines=1)
                | "Parse Lines" >> beam.Map(parse_lines)
                )

        duration = ( lines
                     | "Calculate visit duration" >> beam.ParDo(CalcVisitDuration())
                    )

        ip_map = ( lines
                   | "Get Country from IP" >> beam.ParDo(GetIPCountryOrigin())
                )

        result = (
                    duration
                    | "Map IP to Country" >> beam.Map(
                    map_country_to_ip, ip_map = beam.pvalue.AsDict(ip_map) )
                    | "Average by country" >> beam.CombinePerKey(beam.combiners.MeanCombineFn() )
                    | "Format the output" >> beam.Map(wrap_as_dict)
        )

        result | 'Write to BigQuery' >> bq.WriteToBigQuery(
                            table = my_options.output, schema = SCHEMA,
                            create_disposition = bq.BigQueryDisposition.CREATE_IF_NEEDED,
                            write_disposition=bq.BigQueryDisposition.WRITE_TRUNCATE)
        

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING)
    run()
