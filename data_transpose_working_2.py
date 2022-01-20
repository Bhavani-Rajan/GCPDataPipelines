import logging
import argparse

import apache_beam as beam
import apache_beam.io.gcp.bigquery as bq

INPUT_PROJECT_ID ="my-bq-demo"
INPUT_DATASET_ID="input"
INPUT_TABLE_ID="input_for_transpose"
OUTPUT_PROJECT_ID ="my-bq-demo"
OUTPUT_DATASET_ID="output"
OUTPUT_TABLE_ID="transposed_2"
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
key_field = ['ID']
pivot_field = ['CLASS']
value_field = ['SALES']

class TableSchema():
    def __init__(self,input_schema,key_field,value_field):
        self.output_fields = []
        self.key_schema = {}
        self.value_schema = {}
        self.input_fields = input_schema['fields']
        self.key_field = key_field
        self.value_field = value_field
        self.generateSchemaFromInput()
        
    def addTableField(self,name_field,type_field,mode_field):
        d = {}
        d['name']= name_field
        d['type'] = type_field
        d['mode'] = mode_field
        self.output_fields.append(d)
        
    def getTableSchema(self):
        d = {}
        d['fields'] = self.output_fields
        # print(d)
        return d

    def addKeyFieldsToOutPut(self):
        mode ="REQUIRED"
        for key in self.key_field:
            self.addTableField(key,self.key_schema[key],mode)
            
    # def addPivotFieldsToOutPut(self,list_piv_fields):
    #     mode ='NULLABLE"
    #     for piv in list_piv_fields:
    #         for val in self.value_field:
    #             name = f"{piv}_{val}"
    #             type = self.value_schema[val]
    #             self.addTableField(name,type,mode)
    
    def generateSchemaFromInput(self):
        for d in self.input_fields:
            for field in self.key_field:
                if field == d['name']:
                    self.key_schema[field] = d['type']
            for field in self.value_field:
                if field == d['name']:
                    self.value_schema[field] = d['type']
        



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
        

class SplitAsKV(beam.DoFn):
    def process(self,element):
        d = {}
        key = ""
        for k in key_field:
            d[k] = element[k]
            key  += str(element[k])
        for piv in pivot_field:
            for val in value_field:
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


def getDynamicSchema(element,list_field):
    #    print(list_field)
       d = {}
       d['fields'] = list_field
       return d

class AddPivotFieldsToOutPut(beam.DoFn):
    def process(self, element,schema):
        for piv in element:
            for val in schema.value_field:
                schema.addTableField(f"{piv}_{val}",schema.value_schema[val],"NULLABLE")
        return schema.output_fields
        
        
def run(argv=None):
    parser = argparse.ArgumentParser()
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
        output_table_schema = TableSchema(INPUT_SCHEMA,key_field,value_field)
        output_table_schema.generateSchemaFromInput()
        output_table_schema.addKeyFieldsToOutPut()
        
        input_table_rows = ( p 
                        | "Read BigQuery table" >> bq.ReadFromBigQuery(
                        table = f"{INPUT_PROJECT_ID}:{INPUT_DATASET_ID}.{INPUT_TABLE_ID}" )
                        )
        dynamic_schema = ( input_table_rows
                            | "Get Pivot Schema" >> beam.ParDo(GetPivotValues())        
                            | "Group by pivot field" >> beam.GroupByKey()
                            | "Get unique list" >> beam.ParDo(UniqueList())
                            | "Add Pivot field to Output schema" >> 
                          beam.ParDo(AddPivotFieldsToOutPut(),output_table_schema)
                       )
      
        list_fields = beam.pvalue.AsList(dynamic_schema) 
        

        output_table_rows = ( input_table_rows
                            | "Split as Key Value" >> beam.ParDo(SplitAsKV())
                            | "Group by key" >> beam.GroupByKey()
                            | "Create the table row" >> beam.ParDo(CreateTableRow())
                            )
        
        
        
        (output_table_rows
                    | 'Write to BigQuery' >> bq.WriteToBigQuery(
                            table = f"{OUTPUT_PROJECT_ID}:{OUTPUT_DATASET_ID}.{OUTPUT_TABLE_ID}", 
                            schema = getDynamicSchema,
                            schema_side_inputs = [list_fields],
                            # schema_side_inputs = beam.pvalue.AsList(dynamic_schema),
                            create_disposition = bq.BigQueryDisposition.CREATE_IF_NEEDED,
                            write_disposition=bq.BigQueryDisposition.WRITE_TRUNCATE )
        )
        

if __name__ == "__main__" :
    logging.getLogger().setLevel(logging.WARNING)
    run()
