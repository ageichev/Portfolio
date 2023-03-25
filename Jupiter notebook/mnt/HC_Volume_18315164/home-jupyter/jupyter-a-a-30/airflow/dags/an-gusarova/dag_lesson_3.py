#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
#import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable



# In[2]:


year = 1994 + hash(f'an-gusarova') % 23
vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'



# In[ ]:


default_args = {
    'owner': 'an-gusarova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 24),
}


@dag(default_args=default_args, schedule_interval= '0 17 * * *', catchup=False)
def an_gusarova_airflow_lesson_3():
    @task(retries=3)
    def get_data():
        vg = pd.read_csv(vgsales)
        vg_data = vg.query('Year == @year')
        return vg_data
    
    @task()
    def get_global_sales(vg_data):
        global_sales = vg_data                    .groupby('Name', as_index = False)                    .Global_Sales.sum()                    .sort_values('Global_Sales', ascending = False)                    .head(1)
        return global_sales.to_csv(index = False)
    
    @task()
    def get_Genre_sales(vg_data):
        Genre_EU_sales = vg_data                    .groupby('Genre', as_index = False)                    .EU_Sales.sum()                    .sort_values('EU_Sales', ascending = False)
        return Genre_EU_sales.to_csv(index = False)

    @task()
    def get_NA_sales(vg_data):
        NA_sales = vg_data.query('NA_Sales > 1')                    .groupby('Platform', as_index = False)                    .NA_Sales.sum()                    .sort_values('NA_Sales', ascending = False)
        return NA_sales.to_csv(index = False)
    
    @task()
    def get_JP_sales(vg_data):
        JP_Sales_top_publisher = vg_data.query('JP_Sales > 0')                    .groupby('Publisher', as_index = False)                    .agg({'JP_Sales' : 'mean'})                    .sort_values('JP_Sales', ascending = False)
        return JP_Sales_top_publisher.to_csv(index = False)
    
    @task()
    def get_EU_JP_sales(vg_data):
        EU_sales = vg_data.groupby('Name', as_index = False)                    .agg({'JP_Sales' : 'sum', 'EU_Sales' : 'sum'})
        Better_EU_sales = int(EU_sales.query('EU_Sales > JP_Sales').EU_Sales.count())
        return Better_EU_sales

    @task()
    def print_data(global_sales, Genre_EU_sales, NA_sales, JP_Sales_top_publisher, Better_EU_sales):

        context = get_current_context()
        date = context['ds']

        

        print(f'''Data from vgsales for {date}
                  Top game: {global_sales}
                  Top Genre in EU: {Genre_EU_sales}
                  Top platform > 1 Million in NA: {NA_sales}
                  Top publisher sales in JP: {JP_Sales_top_publisher}
                  EU sales better than JP: {Better_EU_sales}''')

        
 
    vg_data = get_data()
    
    global_sales = get_global_sales(vg_data)
    Genre_EU_sales = get_Genre_sales(vg_data)
    NA_sales = get_NA_sales(vg_data)
    JP_Sales_top_publisher = get_JP_sales(vg_data)
    Better_EU_sales = get_EU_JP_sales(vg_data)

    print_data(global_sales, Genre_EU_sales, NA_sales, JP_Sales_top_publisher, Better_EU_sales)

an_gusarova_airflow_lesson_3 = an_gusarova_airflow_lesson_3()




