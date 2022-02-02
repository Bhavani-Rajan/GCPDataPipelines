from flask import Flask
from read_csv_no_pandas import read_csv

app = Flask(__name__)

@app.route('/')
def home_page():
    return "Home Page "

@app.route('/hello/', methods=['GET','POST'])
def welcome():
    return "Hello World!"

@app.route('/neighbourhood/<name>')
def get_neighbourhood_details(name):

    bucket_name='my-bq-demo-bucket'
    blob_name = 'NYC/neighbourhood_data.csv'
    l_rows = read_csv(bucket_name,blob_name)

    population = 0
    house_price = 0
    coll_edu_percentage = 0

    for d in l_rows:
        if (d['neighbourhood'].str.lower() == name.lower()) :
            population = d['population']
            house_price = d['housing_price_per_sq_ft']
            coll_edu_percentage = d['college_edu_percentage']
    rt_d = {}

    # result = df[df['neighbourhood'].str.lower() == name.lower()]
    # pop_in_l = result['population'].to_list()
    # house_price_l = result['housing_price_per_sq_ft'].to_list()
    # coll_edu_percentage_l = result['college_edu_percentage'].to_list()

    # if len(pop_in_l) > 0:
    #     population = pop_in_l[0]
    #     house_price = house_price_l[0]
    #     coll_edu_percentage = coll_edu_percentage_l[0]
    
    rt_d['neighbourhood'] = name
    rt_d['population'] = population
    rt_d['house_price_sq_ft'] = house_price
    rt_d['coll_edu_percentage'] = coll_edu_percentage

    return rt_d

if __name__ == "__main__":
    app.run()
