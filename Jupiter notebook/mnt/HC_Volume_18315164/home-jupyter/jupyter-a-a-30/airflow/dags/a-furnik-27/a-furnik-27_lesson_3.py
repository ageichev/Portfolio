import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.operators.python import get_current_context
from airflow.decorators import dag, task

user = 'a-furnik-27'
data_url = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 1994 + hash(f'{user}') % 23

default_args = {
    'owner': 'a-furnik-27',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 5),
    'schedule_interval': '30 * * * *'
}

@dag(default_args=default_args, catchup=False)
def tasks_for_lesson_3():

    @task()
    def get_data():
        data = pd.read_csv(data_url)
        data = data[data.Year == year]
        return data

    @task()
    def top_game_in_the_world(data):
        top_game = data[data.Global_Sales ==
                        data.Global_Sales.max()].Name.values[0]
        return top_game

    @task()
    def top_types_of_games_in_eu(data):
        max_eu_sales = data.groupby(
            'Genre', as_index=0).agg({'EU_Sales': 'sum'})
        top_types_of_games_in_eu = max_eu_sales[max_eu_sales.EU_Sales == max_eu_sales.EU_Sales.max(
        )].Genre
        return top_types_of_games_in_eu

    @task()
    def best_na_platform(data):
        best_na_platform = data.query('NA_Sales > 1').groupby(
            'Platform', as_index=False).agg({'Name': 'count'}).rename(columns={'Name': 'count'})
        best_na_platform = best_na_platform[best_na_platform['count']
                                            == best_na_platform['count'].max()]['Platform']
        return best_na_platform

    @task()
    def top_sales_in_japan(data):
        top_sales_in_japan = data.groupby('Publisher', as_index=False).agg(
            {'JP_Sales': 'mean'}).rename(columns={'JP_Sales': 'JP_avg'})
        top_sales_in_japan = top_sales_in_japan[top_sales_in_japan['JP_avg']
                                                == top_sales_in_japan['JP_avg'].max()]['Publisher']
        return top_sales_in_japan

    @task()
    def more_sales_in_europe_than_japan(data):
        more_sales_in_europe_than_japan = data.query(
            'EU_Sales > JP_Sales').Name.count()
        return more_sales_in_europe_than_japan

    @task()
    def print_data(top_game_in_the_world, top_types_of_games_in_eu, best_na_platform, top_sales_in_japan, more_sales_in_europe_than_japan):

        print(f'''Данные за {year} год
                        Самая продаваемая игра: {top_game_in_the_world}
                        Самый продаваемый жанр в Европе: {top_types_of_games_in_eu}
                        Платформы, на которых продались игры с более чем миллионным тиражом в Северной Америке: {best_na_platform}
                        Издатель с самыми высокими продажами в Японии: {top_sales_in_japan}
                        Количество игр, которые продались лучше в Европе, по сравнению с Японией: {more_sales_in_europe_than_japan}''')

    data = get_data()

    game = top_game_in_the_world(data)
    genre = top_types_of_games_in_eu(data)
    platform = best_na_platform(data)
    publisher = top_sales_in_japan(data)
    amount = more_sales_in_europe_than_japan(data)

    print_data(game, genre, platform, publisher, amount)

tasks_for_lesson_3 = tasks_for_lesson_3()
