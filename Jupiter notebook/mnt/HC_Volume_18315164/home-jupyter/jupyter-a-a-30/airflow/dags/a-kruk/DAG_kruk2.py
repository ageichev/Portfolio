import requests
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime
from io import StringIO
#import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

default_args = {
    'owner': 'a.kruk',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 26),
    'schedule_interval': '0 12 * * *'
}

year = 1994 + hash("a-kruk") % 23

@dag(default_args=default_args)
def DAG_kruk2():

    @task()
    def read_data():
        data = pd.read_csv("vgsales.csv")
        data = data.loc[data.Year==year]
        return data

    @task()
    def get_most_popular_world(data):
        return data.groupby('Name').Global_Sales.sum().idxmax()

    @task()
    def get_most_popular_eu(data):
        eu_sales_genre = data \
            .groupby('Genre', as_index=False) \
            .agg({'EU_Sales':'sum'}) \
            .sort_values('EU_Sales', ascending=False)
        q9 = eu_sales_genre.quantile(.9)[0]
        return ", ".join(list(eu_sales_genre.loc[eu_sales_genre.EU_Sales>=q9].Genre))

    @task()
    def get_na_sales_platform(data):
        return data\
                .query('NA_Sales>1') \
                .groupby('Platform') \
                .agg({'Name':'count'}) \
                .rename(columns={'Name':'Count_games'}) \
                .Count_games.idxmax()

    @task()
    def get_publisher_jp(data):
        return data \
                .groupby('Publisher') \
                .agg({'JP_Sales':'mean'}).JP_Sales.idxmax()

    @task()
    def get_eu_jp(data):
        eu_jp = data \
            .groupby('Name', as_index=False) \
            .agg({'EU_Sales':'sum', 'JP_Sales':'sum'})
        return eu_jp.loc[eu_jp.EU_Sales>eu_jp.JP_Sales].shape[0]

    @task()
    def print_data(most_popular_world, most_popular_eu, na_sales_platform, publisher_jp, eu_jp):
            context = get_current_context()
            date = context['ds']

            print(f'''
                Best selling game worldwide in {year}: {most_popular_world}
                Most popular by sales game genre in EU in {year}: {most_popular_eu}
                Most popular platform in North America with game sales over a million editions in {year}: {na_sales_platform}
                Publisher with highest average sales in Japan in {year}: {publisher_jp}
                Number of games sold in Europe better than in Japan in {year}: {eu_jp}''')

    data = read_data()
    most_popular_world = get_most_popular_world(data)
    most_popular_eu = get_most_popular_eu(data)
    na_sales_platform = get_na_sales_platform(data)
    publisher_jp = get_publisher_jp(data)
    eu_jp = get_eu_jp(data)
    print_data(most_popular_world, most_popular_eu, na_sales_platform, publisher_jp, eu_jp)

DAG_kruk2 = DAG_kruk2()