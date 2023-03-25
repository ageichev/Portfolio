import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.operators.python import get_current_context
from airflow.decorators import dag, task

user = 'a-rodionov'
data_url = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 1994 + hash(f'{user}') % 23

default_args = {
    'owner': 'a-rodionov',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 9),
    'schedule_interval': '30 * * * *'
}

@dag(default_args=default_args, catchup=False)
def tasks_lesson_3():

    @task()
    def get_data():
        data = pd.read_csv(data_url)
        data = data[data.Year == year]
        return data

    @task()
    def best_selling_game(data):
        top_game = data[data.Global_Sales ==
                        data.Global_Sales.max()].Name.values[0]
        return top_game

    @task()
    def best_selling_games_eu(data):
        max_eu_sales = data.groupby(
            'Genre', as_index=0).agg({'EU_Sales': 'sum'})
        best_selling_games_eu = max_eu_sales[max_eu_sales.EU_Sales == max_eu_sales.EU_Sales.max(
        )].Genre
        return best_selling_games_eu

    @task()
    def best_platform_na(data):
        best_platform_na = data.query('NA_Sales > 1').groupby(
            'Platform', as_index=False).agg({'Name': 'count'}).rename(columns={'Name': 'count'})
        best_platform_na = best_platform_na[best_platform_na['count']
                                            == best_platform_na['count'].max()]['Platform']
        return best_platform_na

    @task()
    def best_publisher_jp(data):
        best_publisher_jp = data.groupby('Publisher', as_index=False).agg(
            {'JP_Sales': 'mean'}).rename(columns={'JP_Sales': 'JP_avg'})
        best_publisher_jp = best_publisher_jp[best_publisher_jp['JP_avg']
                                                == best_publisher_jp['JP_avg'].max()]['Publisher']
        return best_publisher_jp

    @task()
    def top_games_eu_vs_jp(data):
        top_games_eu_vs_jp = data.query(
            'EU_Sales > JP_Sales').Name.count()
        return top_games_eu_vs_jp

    @task()
    def print_data(best_selling_game, best_selling_games_eu, best_platform_na, best_publisher_jp, top_games_eu_vs_jp):

        print(f'''Данные за {year} год
                        Самая продаваемая игра: {best_selling_game}
                        Самый продаваемый жанр в Европе: {best_selling_games_eu}
                        Платформы, на которых продались игры с более чем миллионным тиражом в Северной Америке: {best_platform_na}
                        Издатель с самыми высокими продажами в Японии: {best_publisher_jp}
                        Количество игр, которые продались лучше в Европе, по сравнению с Японией: {top_games_eu_vs_jp}''')

    data = get_data()

    game = best_selling_game(data)
    genre = best_selling_games_eu(data)
    platform = best_platform_na(data)
    publisher = best_publisher_jp(data)
    amount = top_games_eu_vs_jp(data)

    print_data(game, genre, platform, publisher, amount)

tasks_lesson_3 = tasks_lesson_3()
##


