import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


dataset = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 1994 + hash('m_lunin') % 23

default_args = {
    'owner': 'm.lunin',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 15),
    'schedule_interval': '0 12 * * *'
}



@dag(default_args=default_args)
def m_lunin_28_lesson_3():
    
    @task()
    def get_data():
        vgsales = pd.read_csv(dataset)
        vgsales = vgsales.query('Year == @year')
        return vgsales

    @task()
    def get_global_sales_max(data):
        global_sales_max = data.groupby('Name', as_index=False)\
                               .agg({'Global_Sales': 'sum'})\
                               .sort_values('Global_Sales', ascending=False)\
                               .head(1)
        return global_sales_max
    
    @task()
    def get_eu_sales_max(data):
        eu_sales_max = data.groupby('Genre', as_index=True)\
                           .agg({'NA_Sales': 'sum', 'EU_Sales': 'sum', 'JP_Sales': 'sum', 'Other_Sales': 'sum'})\
                           .idxmax(axis=1)\
                           .reset_index()\
                           .rename(columns={0: 'Region'})\
                           .query('Region == "EU_Sales"')
        if not eu_sales_max.Genre.count():
            eu_sales_max = pd.Series(f'There were not MAX sales genre in Europe in {year}')
        return eu_sales_max
    
    @task()
    def get_na_sales_max_platform(data):
        na_sales_max_platform = data.groupby('Platform',as_index=False)\
                                    .agg({'NA_Sales': 'sum'})\
                                    .query('NA_Sales > 1')
        if not na_sales_max_platform.NA_Sales.count():
            na_sales_max_platform = pd.Series(f'There were not platforms with more then 1M sales in North America in {year}')
        return na_sales_max_platform   
    
    @task()
    def get_jp_sales_highest_mean(data):
        jp_sales_highest_mean = data.groupby('Publisher')\
                                    .agg({'NA_Sales': 'mean', 'EU_Sales': 'mean', 'JP_Sales': 'mean', 'Other_Sales': 'mean'})\
                                    .idxmax(axis=1)\
                                    .reset_index()\
                                    .rename(columns={0: 'Region'})\
                                    .query('Region == "JP_Sales"')
        if not jp_sales_highest_mean.Region.count():
            jp_sales_highest_mean = pd.Series(f'There were not Publishers with MAX average sales in Japan in {year}')
        return jp_sales_highest_mean
    
    @task()
    def get_eu_jp_sales_compare(data):
        eu_jp_sales_compare = data.groupby('Name', as_index=False)\
                                  .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})\
                                  .query('EU_Sales > JP_Sales')\
                                  .EU_Sales.count()
        if not eu_jp_sales_compare:
            eu_jp_sales_result = pd.Series(f'In Japan sales of all games were higher than in Europe in {year}')
        else:
            eu_jp_sales_result = pd.Series(f'In Europe sales of {eu_jp_sales_compare} games were higher than in Japan in {year}')
        return eu_jp_sales_result
    
    
    @task()
    def print_data(data1, data2, data3, data4, data5):

        context = get_current_context()
        date = context['ds']

        print(f'Report date: {date}')
        
        print(f'The best-selling game worldwide in {year} year is:')
        print(data1)
        
        print(f'The MAX sales genre in Europe in {year} year is:')
        print(data2)
        
        print(f'Platforms with more then 1M sales in North America in {year} year is:')
        print(data3)
        
        print(f'Publishers with MAX average sales in Japan in {year} year is:')
        print(data4)
        
        print(data5)

    vgsales = get_data()
    global_sales  = get_global_sales_max(vgsales)
    eu_sales = get_eu_sales_max(vgsales)
    na_sales = get_na_sales_max_platform(vgsales)
    jp_sales = get_jp_sales_highest_mean(vgsales)
    eu_jp_sales = get_eu_jp_sales_compare(vgsales)
    print_data(global_sales, eu_sales, na_sales, jp_sales, eu_jp_sales)

m_lunin_28_lesson_3 = m_lunin_28_lesson_3()