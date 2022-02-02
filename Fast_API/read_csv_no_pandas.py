from google.cloud import storage

# bucket_name='my-bq-demo-bucket'
# blob_name = 'NYC/neighbourhood_data.csv'


def read_csv(bucket_name,blob_name):

    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(blob_name) 

    # with blob.open("rt") as f:
    #     print(f.read())
    l_rows= []
    d_rows = {}
    with blob.open("rt") as f:
        header = f.readline().strip().split(',')
        # print(f" Header ---- {header}")
        
        for line in f:
            values = line.strip().split(',')
            d_rows[values[0].lower()] = dict(zip(header,values))
    return d_rows

if __name__ == '__main__':
    bucket_name='my-bq-demo-bucket'
    blob_name = 'NYC/neighbourhood_data.csv'
    print(read_csv(bucket_name,blob_name))

