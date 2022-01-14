
import logging
import argparse

import apache_beam as beam
import apache_beam.io.gcp.bigquery as bq

PROJECT_ID ="my-bq-demo"
DATASET_ID="input"
TABLE_ID="input_for_transpose"
INPUT_SCHEMA = {
    'fields': [{
        'name': 'ID', 
        'type': 'INTEGER', 
        'mode': 'REQUIRED'
    }, 
    {
        'name': 'CLASS', 
        'type': 'STRING', 
        'mode': 'NULLABLE'
    },
    {
        'name': 'SALES', 
        'type': 'FLOAT', 
        'mode': 'NULLABLE'
        }]
        }

OUTPUT_SCHEMA ={}
key_field = ['ID']
pivot_field = ['CLASS']
value_field = ['SALES']
value_schema = {'SALES':'FLOAT'}
table_id = 'my-bq-demo:output.schema'


class GetPivotValues(beam.DoFn):
    def process(self, element):
        rt_elem = {}
        row = element
        for field in pivot_field:
            rt_elem = {field, row[field]}
            yield rt_elem
        
class UniqueList(beam.DoFn):
    def process(self, element):
        rt_elem =  list(set(element[1])) 
        yield rt_elem
        
class FoldPivotValues(beam.DoFn):
    def process(self, element):
        rt_dict={}
        for piv in element:
            for val in value_schema:
                rt_dict['name'] = f"{piv}_{val}"
                rt_dict['type'] = value_schema[val]
                rt_dict['mode'] = 'NULLABLE'
                return rt_dict
    
def getKeyFieldSchema(list_keys):
    rt_l = []
    for key in list_keys:
        for d in INPUT_SCHEMA['fields']:
            if (d['name'] == key):
                rt_l.append(d)
    return rt_l

def run(argv=None):
    parser = argparse.ArgumentParser()
    # parser.add_argument("--input")
    parser.add_argument("--output")
    # parser.add_argument("--inputTableSpec")
    # parser.add_argument("--outputTableSpec")
    #
    # --keyFields=id,locid \
    # --pivotFields=class,on_sale,state \
    # --valueFields=sale_price,count
    args, beam_args = parser.parse_known_args(argv)

    '''
     * Steps:
     *  1) Read TableRow records from input BigQuery table.
     *  2) Extract pivot schema from TableRow records.
     *  3) Convert to singleton view for sideInput.
     *  4) Create dynamic schema view for writing to output table.
     *  5) Pivot individual rows.
     *  6) Write to output BigQuery table.
    '''

    with beam.Pipeline(argv=beam_args) as p:
        
        list_key_dict = getKeyFieldSchema(key_field)
        key_schema = (p 
                      | "Create Key Schema" >> beam.Create(list_key_dict)
                     )
        
        input_table_rows = ( p 
                        | "Read BigQuery table" >> bq.ReadFromBigQuery(
                        table = f"{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}" )
                        )
        pivoted_schema = ( input_table_rows
                        | "Get Pivot Schema" >> beam.ParDo(GetPivotValues()) 
                        | "Group by pivot field" >> beam.GroupByKey()
                        | "Get unique list" >> beam.ParDo(UniqueList())
                        | "Fold pivot values to columns" >>  beam.ParDo(
                                FoldPivotValues())
                        # | beam.CombineGlobally(beam.combiners.ToListCombineFn())
                            )

        dynamic_schema = ( (key_schema,pivoted_schema) 
                          | 'Merge Schema' >> beam.Flatten()
                          | "Dynamic Schema" >>     beam.CombineGlobally(
                              beam.combiners.ToListCombineFn())
                            )
        
        
        # (input_table_rows
        #     | "Separate key fields" >> 

        # )


        # (input_table_rows | 'Add 10 to Sales' >> beam.ParDo(AddTen()) 
        #     | 'Write to BigQuery' >> bq.WriteToBigQuery(
        #                     table = table_id, schema = OUTPUT_SCHEMA,
        #                     create_disposition = bq.BigQueryDisposition.CREATE_IF_NEEDED,
        #                     write_disposition=bq.BigQueryDisposition.WRITE_TRUNCATE)
        # )
        


        # results = (p | beam.Create() 
                    
                  
        #             | 'Write to BigQuery' >> bq.WriteToBigQuery(
        #                     table = table_id, schema = SCHEMA,
        #                     create_disposition = bq.BigQueryDisposition.CREATE_IF_NEEDED,
        #                     write_disposition=bq.BigQueryDisposition.WRITE_TRUNCATE)
        #             )
        (dynamic_schema | "Write the output" >> beam.io.WriteToText(
                args.output, file_name_suffix=".txt")
        
        )

if __name__ == "__main__" :
    logging.getLogger().setLevel(logging.WARNING)
    run()
