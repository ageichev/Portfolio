import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 1994 + hash(f'{"p-baranov"}') % 23


default_args = {
    'owner': 'p.baranov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 23),
    'schedule_interval': '0 10 * * *'
}    
    
    
@dag(default_args=default_args, catchup=False)
def p_baranov_airflow_lesson_3():
    
    @task()
    def get_data():
        df = pd.read_csv(vgsales).query('Year == @year')
        return df
    
    @task()
    def get_top_Global_Sales(df):
        top_Global_Sales = df.groupby('Name') \
        .agg({'Global_Sales': 'sum'}) \
        .Global_Sales.idxmax()
        return top_Global_Sales
    
    @task()
    def get_top_Games_EU(df):
        top_Games_EU = df.groupby('Genre') \
        .agg({'EU_Sales': 'sum'}) \
        .EU_Sales.idxmax()
        return top_Games_EU
    
    @task()
    def get_top_Platforms_NA(df):
        top_Platforms_NA = df.query('NA_Sales > 1').groupby('Platform') \
        .agg({'Name': 'nunique'}) \
        .rename(columns = {'Name': 'Number'}) \
        .Number.idxmax()
        return top_Platforms_NA
    
    @task()
    def get_top_Publishers_JP(df):
        top_Publishers_JP = df.groupby('Publisher') \
        .agg({'JP_Sales': 'mean'}) \
        .JP_Sales.idxmax()
        return top_Publishers_JP
    
    @task()
    def get_EU_better_JP(df):
        EU_better_JP = df.query('EU_Sales > JP_Sales').shape[0]
        return EU_better_JP
        
    @task()
    def print_data(top_Global_Sales, top_Games_EU, top_Platforms_NA, top_Publishers_JP, EU_better_JP):
        
        context = get_current_context()
        date = context['ds']
        
        print(f"GAME SALES DATA FOR {year}")
        print(f"1. Best global seller: {top_Global_Sales}")
        print(f"2. Best seller genre in EU: {top_Games_EU}")
        print(f"3. Best platform in NA: {top_Platforms_NA}")
        print(f"4. Best publisher in JP: {top_Publishers_JP}")
        print(f"5. Number of games that sold better in EU than in JP: {EU_better_JP}")
    
    
    
    top_data = get_data()
    top_Global_Sales = get_top_Global_Sales(top_data)
    top_Games_EU = get_top_Games_EU(top_data)
    top_Platforms_NA = get_top_Platforms_NA(top_data)
    top_Publishers_JP = get_top_Publishers_JP(top_data)
    EU_better_JP = get_EU_better_JP(top_data)
    print_data(top_Global_Sales, top_Games_EU, top_Platforms_NA, top_Publishers_JP, EU_better_JP)
    
p_baranov_airflow_lesson_3 = p_baranov_airflow_lesson_3()