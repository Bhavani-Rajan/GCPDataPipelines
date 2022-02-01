from flask import Flask
import pandas as pd

app = Flask(__name__)

@app.route('/')
def home_page():
    return "Home Page "

@app.route('/hello/', methods=['GET','POST'])
def welcome():
    return "Hello World!"

@app.route('/neighbourhood/<name>')
def get_neighbourhood_details(name):
    df = pd.read_csv('gs://my-bq-demo-bucket/NYC/neighbourhood_data.csv')

    population = 0
    house_price = 0
    coll_edu_percentage = 0

    rt_d = {}

    result = df[df['neighbourhood'].str.lower() == name.lower()]
    pop_in_l = result['population'].to_list()
    house_price_l = result['housing_price_per_sq_ft'].to_list()
    coll_edu_percentage_l = result['college_edu_percentage'].to_list()

    if len(pop_in_l) > 0:
        population = pop_in_l[0]
        house_price = house_price_l[0]
        coll_edu_percentage = coll_edu_percentage_l[0]
    
    rt_d['neighbourhood'] = name
    rt_d['population'] = population
    rt_d['house_price_sq_ft'] = house_price
    rt_d['coll_edu_percentage'] = coll_edu_percentage

    return rt_d

if __name__ == "__main__":
    app.run()
