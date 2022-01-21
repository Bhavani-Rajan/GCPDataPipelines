import argparse
import logging
import csv
import sys 

import apache_beam as beam
import apache_beam.io.gcp.bigquery as bq

SCHEMA_RAW = {'fields':[  {"mode": "REQUIRED","name": "id","type": "INTEGER"},
                    #   {"mode": "NULLABLE","name": "name","type": "STRING"},
                      {"mode": "NULLABLE", "name": "host_id","type": "INTEGER"},
                      {"mode": "NULLABLE","name": "host_name","type": "STRING"},
                      {"mode": "NULLABLE","name": "neighbourhood_group","type": "STRING"},
                      {"mode": "NULLABLE","name": "neighbourhood","type": "STRING"},
                      {"mode": "NULLABLE","name": "latitude","type": "FLOAT"},
                      {"mode": "NULLABLE","name": "longitude","type": "FLOAT"},
                      {"mode": "NULLABLE","name": "room_type", "type": "STRING"},
                      {"mode": "NULLABLE","name": "price", "type": "INTEGER" },
                      {"mode": "NULLABLE","name": "minimum_nights","type": "INTEGER"},
                      {"mode": "NULLABLE","name": "number_of_reviews","type": "INTEGER"},
                      {"mode": "NULLABLE","name": "last_review","type": "DATE"},
                      {"mode": "NULLABLE","name": "reviews_per_month","type": "FLOAT"},
                      {"mode": "NULLABLE","name": "calculated_host_listings_count", "type": "INTEGER"},
                      {"mode": "NULLABLE","name": "availability_365", "type": "INTEGER"}
                ]}
SCHEMA_TRANSFORMED = {'fields':[  {"mode": "NULLABLE","name": "neighbourhood","type": "STRING"},
                      {"mode": "NULLABLE","name": "count_listings","type": "INTEGER"}
                      ]}
# SCHEMA_RAW = 'id:INTEGER,host_id:INTEGER,host_name:STRING,neighbourhood_group:STRING, \
#             neighbourhood:STRING,latitude:FLOAT,longitude:FLOAT,room_type:STRING, \
#             price:INTEGER,minimum_nights:INTEGER,number_of_reviews:INTEGER,	\
#             last_review:DATE,reviews_per_month:FLOAT, \
#             calculated_host_listings_count:INTEGER,	availability_365:INTEGER'

# SCHEMA_TRANSFORMED = 'neighbourhood:STRING, count_listings:INTEGER'
table_id_raw = 'my-bq-demo:NYC.raw_no_name'
table_id_transformed = 'my-bq-demo:NYC.transformed'




def parse_line(element):
    values = element.split(",")
    row = dict(
            zip(('id','host_id','host_name','neighbourhood_group','neighbourhood',
            'latitude',	'longitude','room_type','price','minimum_nights','number_of_reviews',
            'last_review',	'reviews_per_month','calculated_host_listings_count','availability_365'),
                values))
    return row


class SplitRowByNeighbourhood(beam.DoFn):
    def process(self,element):
        neighbourhood = element['neighbourhood']
        id = element['id']
        yield (neighbourhood,id)

def covertToTableRow(element):
    values = [element[0],element[1]]
    row = dict(
            zip(('neighbourhood','count_listings'),
                values))
    return row

def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()

    parser.add_argument('--input')
    # parser.add_argument('--output')

    # Parse arguments from the command line.
    args, pipeline_args = parser.parse_known_args(argv)

    # DataIngestion is a class we built in this script to hold the logic for
    # transforming the file into a BigQuery table.
   

    with beam.Pipeline(argv=pipeline_args) as p:
        input_table_rows = ( 
                    p
                    | 'Read from a File' >> beam.io.ReadFromText(args.input,
                                                                skip_header_lines=1)
                    | 'String To BigQuery Row' >> beam.Map(parse_line) 
                    )

        
        transformed_data = (input_table_rows
                             | "Tranform the data" >> beam.ParDo(SplitRowByNeighbourhood())
                             | "Combine by neighborhood" >> beam.CombinePerKey(beam.combiners.CountCombineFn())
                             | "Convert as Table Row" >> beam.Map(covertToTableRow)
                            )
       
        (input_table_rows  
         | 'Write the Raw file to BigQuery' >> bq.WriteToBigQuery(
                    table = table_id_raw, schema = SCHEMA_RAW,
                    create_disposition = bq.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition= bq.BigQueryDisposition.WRITE_TRUNCATE)
                    )

        (transformed_data  
         | 'Write the transformed file to BigQuery' >> bq.WriteToBigQuery(
                    table = table_id_transformed, schema = SCHEMA_TRANSFORMED,
                    create_disposition = bq.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition= bq.BigQueryDisposition.WRITE_TRUNCATE)
                    )
    # schema_side_inputs if tuple need to be passed in as table schema
    # result = p.run()
    # result.wait_until_finish()



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.WARNING)
    run()