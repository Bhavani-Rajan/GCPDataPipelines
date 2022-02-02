import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
from read_csv_no_pandas import read_csv

app = FastAPI(
    title='Neighbourhood Info',
    description='given a neighbourhood name returns the details for NYC',
    version='0.1',
    docs_url='/',
)


@app.get("/hello")
def read_root():
    return {"Hello": "World"}


@app.get("/info/{neighbourhood_name}")
def read_item(neighbourhood_name: str):
    bucket_name='my-bq-demo-bucket'
    blob_name = 'NYC/neighbourhood_data.csv'
    d_rows = read_csv(bucket_name,blob_name)

    name = "Not found"
    population = 0
    house_price = 0
    coll_edu_percentage = 0
    

    if (neighbourhood_name.lower() in d_rows.keys()):
         d = d_rows[neighbourhood_name.lower()]
         name = d['neighbourhood']
         population = d['population']
         house_price = d['housing_price_per_sq_ft']
         coll_edu_percentage = d['college_edu_percentage']

    rt_d = {}
    rt_d['neighbourhood'] = name
    rt_d['population'] = population
    rt_d['house_price_sq_ft'] = house_price
    rt_d['coll_edu_percentage'] = coll_edu_percentage

    return rt_d


# if __name__ == '__main__':
#     uvicorn.run(app)