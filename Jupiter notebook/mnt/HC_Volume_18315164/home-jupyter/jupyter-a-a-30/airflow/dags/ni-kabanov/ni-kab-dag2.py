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

default_args = {
    'owner': 'ni-kabanov',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 26),
    'schedule_interval': '59 23 * * *'
}

@dag(default_args=default_args, catchup=False)
def kabanov_dag_2():
    @task(retries=3)
    def get_data():
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        login = 'ni-kabanov'
        year = year = 1994 + hash(f'{login}') % 23
        df = df.dropna()
        df = df[df.Year == year]
        return df

    
    @task(retries=4, retry_delay=timedelta(10))
    def popular_game(df):
        top_sell = df.groupby('Name').Global_Sales.sum().reset_index() \
                     .sort_values('Global_Sales', ascending=False) \
                     .head(1).Name.values[0]
        return top_sell

    
    @task()
    def genre_EU(df):
        popular_EU = df[['Genre', 'EU_Sales']].groupby('Genre').sum() \
                                 .sort_values('EU_Sales', ascending=False) \
                                 .reset_index()
        bests = popular_EU.EU_Sales.quantile(q=0.999)
        popular_EU = popular_EU[popular_EU.EU_Sales >= bests]['Genre'].to_list()
        return popular_EU

    
    @task()
    def million(df):
        mil_sell_NA = df.query("NA_Sales > 1").groupby('Platform').Name.count().reset_index()
        mil_sell_NA = mil_sell_NA[mil_sell_NA.Name == mil_sell_NA.Name.max()].Platform.to_list()
        return mil_sell_NA

    
    @task()
    def top_JP(df):
        top_mean_JP = df[['Publisher', 'JP_Sales']].groupby('Publisher').agg({'JP_Sales': 'mean'}) \
                             .reset_index().sort_values('JP_Sales', ascending=False)
        bests = top_mean_JP.JP_Sales.quantile(q=0.999)
        top_mean_JP = top_mean_JP[top_mean_JP.JP_Sales >= bests]['Publisher'].to_list()
        return top_mean_JP
    
    
    @task()
    def EUvsJP(df):
        EUvsJP = df[['Name', 'EU_Sales', 'JP_Sales']].groupby('Name').sum().reset_index()
        EUvsJP = (EUvsJP.EU_Sales > EUvsJP.JP_Sales).sum()
        return EUvsJP

    @task()
    def print_data(top_sell, popular_EU, mil_sell_NA, top_mean_JP, EUvsJP):
        print(f"Most sold game:")
        print(top_sell)

        print(f'Popular genre(s) EU:')
        print(*popular_EU)

        print('Platform(s) with million sold games in NA:')
        print(*mil_sell_NA)

        print('Best publisher(s) in JP:')
        print(*top_mean_JP)

        print(f'The number of games that were sold better in Europe than in Japan:')
        print(EUvsJP)
        
        
    df = get_data()
    top_sell = popular_game(df)
    popular_EU = genre_EU(df)
    mil_sell_NA = million(df)
    top_mean_JP = top_JP(df)
    EUvsJP = EUvsJP(df)
    printdata = print_data(top_sell, popular_EU, mil_sell_NA, top_mean_JP, EUvsJP)

    
kabanov_dag_2 = kabanov_dag_2() 
