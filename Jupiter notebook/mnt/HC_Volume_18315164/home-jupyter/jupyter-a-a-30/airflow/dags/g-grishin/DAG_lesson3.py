import pandas as pd
from datetime import timedelta, datetime
from airflow.decorators import dag, task

default_args = {
    'owner': 'g-grishin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 6),
    'schedule_interval': '00 12 * * *'
}

login = 'g-grishin'
year = 1994 + hash(f'{login}') % 23
file = 'vgsales.csv'


@dag(default_args=default_args,
     catchup=False)
def grishin_dag_lesson3():
    @task(retries=3)
    def get_data():
        data = pd.read_csv(file) \
            .query('Year == @year')

        return data


    @task(retries=2)
    def get_most_popular_game(data):
        most_popular_game = data \
            .groupby('Name') \
            .agg({'Global_Sales': 'sum'}) \
            .sort_values('Global_Sales', ascending=False) \
            .head(1) \
            .reset_index() \
            .Name \
            .to_string(index=False)

        return most_popular_game


    @task(retries=2)
    def get_most_popular_eu_genres(data):
        most_popular_eu_genres = data \
            .groupby('Genre') \
            .agg({'EU_Sales': 'sum'}) \
            .sort_values('EU_Sales', ascending=False) \
            .head(3)

        return most_popular_eu_genres


    @task(retries=2)
    def get_most_popular_na_platforms(data):
        most_popular_na_platforms = data \
            .query('NA_Sales > 1') \
            .groupby('Platform') \
            .agg({'Name': 'count'})

        return most_popular_na_platforms.query("Name == @most_popular_na_platforms.Name.max()").reset_index().Platform


    @task(retries=2)
    def get_most_popular_jp_publisher(data):
        most_popular_jp_publisher = data \
            .groupby('Publisher') \
            .agg({'JP_Sales': 'mean'}) \
            .sort_values('JP_Sales', ascending=False) \
            .idxmax() \
            .to_string(index=False)

        return most_popular_jp_publisher


    @task(retries=2)
    def get_games(data):
        number_of_games = data \
            .query("EU_Sales > JP_Sales") \
            .shape[0]

        return number_of_games


    @task(retries=2)
    def print_data(most_popular_game,
                   most_popular_eu_genres,
                   most_popular_na_platforms,
                   most_popular_jp_publisher,
                   number_of_games,
                   year):
        year = year

        print(f'The most popular game in {year} worldwide:')
        print(most_popular_game)

        print(f'The most popular game genres in {year} in EU:')
        print(most_popular_eu_genres)

        print(f'The most popular game platforms in {year} in North America:')
        print(most_popular_na_platforms)

        print(f'The most popular game publishers in {year} in Japan:')
        print(most_popular_jp_publisher)

        print(f'Number of games better sold in EU than in Japan in {year}:')
        print(number_of_games)

    data = get_data()

    most_popular_game = get_most_popular_game(data)
    most_popular_eu_genres = get_most_popular_eu_genres(data)
    most_popular_na_platforms = get_most_popular_na_platforms(data)
    most_popular_jp_publisher = get_most_popular_jp_publisher(data)
    number_of_games = get_games(data)

    print_data(most_popular_game,
               most_popular_eu_genres,
               most_popular_na_platforms,
               most_popular_jp_publisher,
               number_of_games,
               year)


grishin_dag_lesson3 = grishin_dag_lesson3()
