import argparse
import logging
import csv
import sys 
import requests

import apache_beam as beam
import apache_beam.io.gcp.bigquery as bq

SCHEMA_RAW = {'fields':[  {"mode": "REQUIRED","name": "id","type": "STRING"},
                      {"mode": "NULLABLE","name": "name","type": "STRING"},
                      {"mode": "NULLABLE", "name": "host_id","type": "STRING"},
                      {"mode": "NULLABLE","name": "host_name","type": "STRING"},
                      {"mode": "NULLABLE","name": "neighbourhood_group","type": "STRING"},
                      {"mode": "NULLABLE","name": "neighbourhood","type": "STRING"},
                      {"mode": "NULLABLE","name": "latitude","type": "FLOAT"},
                      {"mode": "NULLABLE","name": "longitude","type": "FLOAT"},
                      {"mode": "NULLABLE","name": "room_type", "type": "STRING"},
                      {"mode": "NULLABLE","name": "price", "type": "FLOAT" },
                      {"mode": "NULLABLE","name": "minimum_nights","type": "INTEGER"},
                      {"mode": "NULLABLE","name": "number_of_reviews","type": "INTEGER"},
                      {"mode": "NULLABLE","name": "last_review","type": "STRING"},
                      {"mode": "NULLABLE","name": "reviews_per_month","type": "FLOAT"},
                      {"mode": "NULLABLE","name": "calculated_host_listings_count", "type": "INTEGER"},
                      {"mode": "NULLABLE","name": "availability_365", "type": "INTEGER"}
                ]}
            
SCHEMA_TRANSFORMED = {'fields':[  {"mode": "NULLABLE","name": "neighbourhood","type": "STRING"},
                      {"mode": "NULLABLE","name": "count_listings","type": "INTEGER"},
                      {"mode": "NULLABLE","name": "population","type": "INTEGER"},
                      {"mode": "NULLABLE","name": "house_price_sq_ft","type": "INTEGER"},
                      {"mode": "NULLABLE","name": "coll_edu_percentage","type": "FLOAT"}
                      ]}
# SCHEMA_RAW = 'id:string,name:string,host_id:string,host_name:string, \
#             neighbourhood_group:string,neighbourhood:string, \
#             latitude:float,longitude:float,room_type:string, \
#             price:float,minimum_nights:integer,number_of_reviews:integer, \
#             last_review:string,reviews_per_month:float, \
#             calculated_host_listings_count:integer,availability_365:integer'

# SCHEMA_TRANSFORMED = 'neighbourhood:STRING, count_listings:INTEGER'
table_id_raw = 'my-bq-demo:NYC.raw_with_name'
table_id_transformed = 'my-bq-demo:NYC.transformed_with_info'
input_table_id = 'my-bq-demo:input.AB_NYC_with_names'
url_neigh_data = 'https://my-bq-demo.uw.r.appspot.com/neighbourhood/'


def parse_line(element):
    values = element.split(",")
    row = dict(
            zip(('id','name','host_id','host_name','neighbourhood_group','neighbourhood',
            'latitude',	'longitude','room_type','price','minimum_nights','number_of_reviews',
            'last_review',	'reviews_per_month','calculated_host_listings_count','availability_365'),
                values))
    return row


class SplitRowByNeighbourhood(beam.DoFn):
    def process(self,element):
        neighbourhood = element['neighbourhood']
        id = element['id']
        yield (neighbourhood,id)

def get_neighbourhood_details(element):
    # url_neigh_data = 'https://my-bq-demo.uw.r.appspot.com/neighbourhood/'
    name = element
    url = f"{url_neigh_data}{name}"
    response = requests.get(url)
 
    return response.json()

def covertToTableRow(element):
    # values = [element[0],element[1]]
    values = []
    neighbourhood_name = element[0]
    count_listings = element[1]

    neighbourhood_details = get_neighbourhood_details(neighbourhood_name)
    
    population = neighbourhood_details["population"]
    house_price_sq_ft = neighbourhood_details["house_price_sq_ft"]
    coll_edu_percentage = neighbourhood_details["coll_edu_percentage"]

    values.append(neighbourhood_name)
    values.append(count_listings)
    values.append(population)
    values.append(house_price_sq_ft)
    values.append(coll_edu_percentage)

    row = dict(
            zip(('neighbourhood','count_listings','population','house_price_sq_ft','coll_edu_percentage'),
                values))
    return row



def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()

    # parser.add_argument('--input')
    # parser.add_argument('--output')

    # Parse arguments from the command line.
    args, pipeline_args = parser.parse_known_args(argv)

    # DataIngestion is a class we built in this script to hold the logic for
    # transforming the file into a BigQuery table.
    # query = f" SELECT * FROM {input_table_id}"
    # qry_pipe(
    #                     # table = input_table_id
    #                     query = query,
    #                     output = table_id_raw,
    #                     output_schema = SCHEMA_RAW
    #                     )
    # qry_pipe.run_bq_pipeline(argv)
    with beam.Pipeline(argv=pipeline_args) as p:
        # query = f" SELECT * FROM {input_table_id}"
        # qry_pipe.run_bq_pipeline() --query query --output table_id_raw --output_schema = SCHEMA_RAW
        # input_table_rows = ( 
        #             p
        #             | 'Read from a File' >> beam.io.ReadFromText(args.input,
        #                                                         skip_header_lines=1)
        #             | 'String To BigQuery Row' >> beam.Map(parse_line) 
        #             )

        input_table_rows = ( 
                    p
                    | 'Read from a File' >> bq.ReadFromBigQuery(table = input_table_id)
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

    

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.WARNING)
    run()