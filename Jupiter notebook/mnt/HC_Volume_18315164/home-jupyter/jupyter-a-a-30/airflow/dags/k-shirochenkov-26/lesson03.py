import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from zipfile import ZipFile

from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

TOP_1M_DOMAINS = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'k-shirochenkov-26',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 20),
    'schedule_interval': '30 10 * * *'
}


@dag(default_args=default_args, catchup=False)
def k_shirochenkov_lesson03():
    @task()
    def get_data():
        df = pd.read_csv(TOP_1M_DOMAINS, index_col=0)
        data = df.to_csv()
        return data


    def get_year():
        year = 1994 + hash(f'k-shirochenkov-26') % 23
        print(f'Year={year}')
        return year

    @task()
    def get_most_global_sold_game(data, year):
        df = pd.read_csv(StringIO(data))
        game = df[df.Year == year].sort_values('Global_Sales', ascending=False).head(1).Name.values[0]
        return game

    @task()
    def get_most_genres_eu(data, year):
        df = pd.read_csv(StringIO(data))
        eu_sales = df[df.Year == year].groupby('Genre', as_index=False).agg(sales=('EU_Sales', 'sum')).sort_values(
            'sales', ascending=False)
        most_sales = eu_sales["sales"].values[0]
        genres = eu_sales[eu_sales.sales == most_sales]["Genre"].tolist()
        return genres

    @task()
    def get_platform_na(data, year):
        df = pd.read_csv(StringIO(data))
        na_sales = df[(df.Year == year) & (df.NA_Sales > 1)].groupby('Publisher', as_index=False).agg(
            {'Name': 'count'}).sort_values('Name', ascending=False)
        most_games = na_sales["Name"].values[0]
        platform = na_sales[na_sales.Name == most_games]["Publisher"].tolist()
        return platform

    @task()
    def get_avg_sales_jp(data, year):
        df = pd.read_csv(StringIO(data))
        jp_sales = df[(df.Year == year)].groupby("Publisher", as_index=False).agg(
            sales=('JP_Sales', 'mean')).sort_values('sales', ascending=False)
        most_sales = jp_sales["sales"].values[0]
        publisher = jp_sales[jp_sales.sales == most_sales]["Publisher"].tolist()
        return publisher

    @task()
    def get_better_sales(data, year):
        df = pd.read_csv(StringIO(data))
        df["eu_games"] = df.apply(lambda x: 1 if x["EU_Sales"] > x["JP_Sales"] else 0, axis=1)
        sales = df[(df.Year == year) & (df.eu_games == 1)].Name.count() - df[(df.Year == year) & (df.eu_games == 0)].Name.count()
        return sales

    @task()
    def print_data(year, game, genres_eu, platform_na, avg_sales_jp, better_sales):
        сontext = get_current_context()
        date = сontext['ds']

        print(f'Report on date: {date}')
        print(f'Top sold game overall in {year} year: {game}')
        print(f'Top sold genres in {year} year: {genres_eu}')
        print(f'Top platform in {year} year which sold more than 1M in NA region: {platform_na}')
        print(f'Publishers with best average sales in {year} year in Japan: {avg_sales_jp}')
        print(f'In {year} year {better_sales} games more were sold in EU rather than JP')


    game_data = get_data()
    our_year = get_year()
    top_sold_game = get_most_global_sold_game(game_data, our_year)
    most_genres_eu = get_most_genres_eu(game_data, our_year)
    most_platform_na = get_platform_na(game_data, our_year)
    most_avg_sales_jp = get_avg_sales_jp(game_data, our_year)
    number_better_sales = get_better_sales(game_data, our_year)
    print_data(our_year, top_sold_game, most_genres_eu, most_platform_na, most_avg_sales_jp, number_better_sales)


k_shirochenkov_lesson03 = k_shirochenkov_lesson03()
