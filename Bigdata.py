import json
from datetime import datetime

from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import requests


def thai_food():
    url = 'https://raw.githubusercontent.com/thangman22/thai-food-open-data/master/food.json'
    response = requests.get(url)
    dataset = response.json()
    #with open('data2.json', 'w') as f:
        #json.dump(data2, f)
    return dataset


def save_data_into_db():
    #mysql_hook = MySqlHook(mysql_conn_id='app_db')
    dataset = thai_food()
    for data in dataset:
        import mysql.connector
        db = mysql.connector.connect(host='44.197.22.37',user='root',passwd='password',db='thai_food')

        cursor = db.cursor()
        name = data['name']
        eng_name = data['eng_name']
        rice = data['rice']
        spicy = data['spicy']
        seafood = data['seafood']
        green_level = data['green_level']
        avg_calories = data['avg_calories']
        cuisine = data['cuisine']


        cursor.execute('INSERT INTO thai_food_table (name,eng_name,rice,spicy,seafood,green_level,avg_calories,cuisine)'
                  'VALUES("%s","%s","%s","%s","%s","%s","%s","%s")',
                   (name,eng_name,rice,spicy,seafood,green_level,avg_calories,cuisine))

        db.commit()
        print("Record inserted successfully into  table")
        cursor.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),

}
with DAG('thai_food_data_pipeline',
         schedule_interval='@daily',
         default_args=default_args,
         description='A simple data pipeline for btc report',
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='thai_food',
        python_callable = thai_food
    )

    t2 = PythonOperator(
        task_id='save_data_into_db',
        python_callable=save_data_into_db
    )

    t1 >> t2
