import logging
import argparse

import apache_beam as beam
import apache_beam.io.gcp.bigquery as bq

# PROJECT_ID ="my-bq-demo"
# DATASET_ID="input"
# TABLE_ID="input_for_transpose"
INPUT_PROJECT_ID ="my-bq-demo"
INPUT_DATASET_ID="input"
INPUT_TABLE_ID="input_for_transpose"
OUTPUT_PROJECT_ID ="my-bq-demo"
OUTPUT_DATASET_ID="output"
OUTPUT_TABLE_ID="transposed"
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
OUTPUT_SCHEMA = {}
OUTPUT_SCHEMA['fields'] = []
key_field = ['ID']
pivot_field = ['CLASS']
value_field = ['SALES']


def generateSchemaFromInput(input_schema,key_field,value_field):
    key_schema = {}
    value_schema = {}
    for d in input_schema['fields']:
        for field in key_field:
            if d['name'] == field:
                key_schema[field] = d['type']
        for field in value_field:
            if d['name'] == field:
                value_schema[field] = d['type']
    return key_schema,value_schema

class getKeySchema(beam.DoFn):
    def process(self, element, key_schema):
        rt_dict={}
        rt_dict['mode'] = 'REQUIRED'
        rt_dict['name'] = element
        rt_dict['type'] = key_schema[element]
        yield rt_dict


class GetPivotValues(beam.DoFn):
    def process(self, element):
        rt_elem = {}
        row = element
        for field in pivot_field:
            rt_elem = (field, row[field])
            yield rt_elem
        
class UniqueList(beam.DoFn):
    def process(self, element):
        rt_elem =  list(set(element[1])) 
        yield rt_elem
        
class FoldPivotValues(beam.DoFn):
    def process(self, element,value_schema):
        rt_dict={}
        for piv in element:
            for val in value_field:
                rt_dict['name'] = f"{piv}_{val}"
                rt_dict['type'] = value_schema[val]
                rt_dict['mode'] = 'NULLABLE'
                yield rt_dict
    
class SplitAsKV(beam.DoFn):
    def process(self,element):
        d = {}
        key = ""
        for k in key_field:
            d[k] = element[k]
            key  += str(element[k])
        for piv in pivot_list:
            for val in value_list:
                name = f"{element[piv]}_{val}"
                d[name] = element[val]
        yield (key,d)

class CreateTableRow(beam.DoFn):
    def process(self,element):
        d = {}
        for elem_d in element[1]:
            for k in elem_d.keys():
                d[k] = elem_d[k]
        yield d

def addFieldsToOutputSchema(element):
    schema = {}
    l_fields = beam.pvalue.AsList(element)
    # print(f"inside add fields {element} ")
    schema['fields'] = l_fields
    return schema

# class WriteToBQ(beam.DoFn):
#     def process(self,element,schema):
#         bq.WriteToBigQuery(
#                             table = f"{OUTPUT_PROJECT_ID}:{OUTPUT_DATASET_ID}.{OUTPUT_TABLE_ID}", 
#                             schema = schema,
#                             create_disposition = bq.BigQueryDisposition.CREATE_IF_NEEDED,
#                             write_disposition=bq.BigQueryDisposition.WRITE_TRUNCATE )
#         )
#         yield None

# def writeOutput():
#     with beam.Pipeline() as p:
#         (p 
#                     | beam.Create([{'ID':'123','AAA_SALES':22.44,'BBB_SALES':24.6}
#                                         ]) 
#                     | 'Write to BigQuery' >> bq.WriteToBigQuery(
#                             table = out_table_id, 
#                             schema = OUTPUT_SCHEMA,
#                             create_disposition = bq.BigQueryDisposition.CREATE_IF_NEEDED,
#                             write_disposition=bq.BigQueryDisposition.WRITE_TRUNCATE )
#         )

# def genSchema():
#     key,val = generateSchemaFromInput(key_field,value_field,INPUT_SCHEMA)
#     key_schema = key
#     value_schema = val
#     OUTPUT_SCHEMA['fields'] = getKeyFieldSchema(key_field)

def run(argv=None):
    parser = argparse.ArgumentParser()
    # parser.add_argument("--input")
    # parser.add_argument("--output")
    # parser.add_argument("--output1")
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
        key_schema,value_schema = generateSchemaFromInput(INPUT_SCHEMA,key_field,value_field)
        input_table_rows = ( p 
                        | "Read BigQuery table" >> bq.ReadFromBigQuery(
                        table = f"{INPUT_PROJECT_ID}:{INPUT_DATASET_ID}.{INPUT_TABLE_ID}" )
                        )
        pivoted_schema = ( input_table_rows
                            | "Get Pivot Schema" >> beam.ParDo(GetPivotValues())        
                            | "Group by pivot field" >> beam.GroupByKey()
                            | "Get unique list" >> beam.ParDo(UniqueList())
                            | "Fold pivot values to columns" >> beam.ParDo(
                                FoldPivotValues(),value_schema= value_schema)
                       )
      
        key_schema = ( p | "Read the id fields from key field" >> beam.Create(
                        key_field )
                      | "Add Key field" >> beam.ParDo(getKeySchema(),key_schema= key_schema)
                     )
        
        dynamic_schema = ( (key_schema,pivoted_schema) 
                            | "Merge two schema" >> beam.Flatten()
                            # | "Add to output table schema" >> beam.Map(addFieldsToOutputSchema)
                            # | "Combine globally" >> beam.CombineGlobally(beam.pvalue.AsList())
                         )

        out_schema =  addFieldsToOutputSchema(dynamic_schema)
        # OUTPUT_SCHEMA['fields'] = beam.pvalue.AsList(dynamic_schema)
        # OUTPUT_SCHEMA['fields'] = dynamic_schema
        # dynamic_schema.wait_until_finish()

        output_table_rows = ( input_table_rows
                            | "Split as Key Value" >> beam.ParDo(SplitAsKV())
                            | "Group by key" >> beam.GroupByKey()
                            | "Create the table row" >> beam.ParDo(CreateTableRow())
                            )
        

        # (output_table_rows
        #             | 'Write to BigQuery' >> beam.ParDo(WriteToBQ(),schema=OUTPUT_SCHEMA)
        # )
        (output_table_rows
                    | 'Write to BigQuery' >> bq.WriteToBigQuery(
                            table = f"{OUTPUT_PROJECT_ID}:{OUTPUT_DATASET_ID}.{OUTPUT_TABLE_ID}", 
                            schema = out_schema,
                            # schema_side_inputs = out_schema,
                            create_disposition = bq.BigQueryDisposition.CREATE_IF_NEEDED,
                            write_disposition=bq.BigQueryDisposition.WRITE_TRUNCATE )
        )
        

if __name__ == "__main__" :
    logging.getLogger().setLevel(logging.WARNING)
    # genSchema()
    run()
    # writeOutput()

