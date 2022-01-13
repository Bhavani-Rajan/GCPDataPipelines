import logging
import argparse
from datetime import datetime
import requests

import apache_beam as beam

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

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input")
    parser.add_argument("--output")
    args, beam_args = parser.parse_known_args(argv)

    with beam.Pipeline(argv=beam_args) as p:
        lines = ( p
                | "Read File" >> beam.io.ReadFromText(args.input, skip_header_lines=1)
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
                    | "Format the output" >> beam.Map(lambda element: ",".join(map(str, element)))
        )

        # result | beam.Map(print)

        # TO generate the ouput file
        result | "Write the output" >> beam.io.WriteToText(
                args.output, file_name_suffix=".csv"
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING)
    run()
