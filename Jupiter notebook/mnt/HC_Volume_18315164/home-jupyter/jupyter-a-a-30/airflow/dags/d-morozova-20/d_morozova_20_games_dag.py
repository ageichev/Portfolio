from datetime import datetime, timedelta
import pandas as pd
from airflow.decorators import dag, task

LOGIN = 'd-morozova-20'
FILE_PATH = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year_of_interest = 1994 + hash(f'{LOGIN}') % 23

default_args = {
    'owner': 'd.morozova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 2),
    'schedule_interval': '07 16 * * *'
}


@dag(default_args=default_args, catchup=False)
def d_morozova_20_games_dag():

    # Загружаем и фильтруем данные
    @task(retries=3)
    def get_data(year_of_interest):
        data = pd.read_csv(FILE_PATH)
        data = data[data['Year'] == year_of_interest]
        return data

    # Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def get_best_selling_game_global(data):
        game = data.sort_values('Global_Sales', ascending=False)['Name'].values[0]
        return game

    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def get_most_popular_genres_eu(data):
        sales_by_genre = data.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'})
        max_sum = sales_by_genre['EU_Sales'].max()
        games = list(sales_by_genre[sales_by_genre['EU_Sales'] == max_sum]['Genre'].values)
        return games

    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    # Перечислить все, если их несколько
    @task()
    def get_most_popular_platforms_na(data):
        platforms_games = data[data['NA_Sales'] > 1].groupby('Platform', as_index=False).agg({'Name': 'count'})
        games_max = platforms_games['Name'].max()
        platforms = list(platforms_games[platforms_games['Name'] == games_max]['Platform'].values)
        return platforms

    # У какого издателя самые высокие средние продажи в Японии?
    # Перечислить все, если их несколько
    @task()
    def get_top_publishers_jp(data):
        publisher_mean_sales = data.groupby('Publisher', as_index=False).agg({'JP_Sales': 'mean'})
        sales_max = publisher_mean_sales['JP_Sales'].max()
        publishers = list(publisher_mean_sales[publisher_mean_sales['JP_Sales'] == sales_max]['Publisher'].values)
        return publishers

    # Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def get_games_sold_better_eu_than_jp(data):
        return data[data['EU_Sales'] > data['JP_Sales']]['Name'].nunique()

    # Собираем и выводим все ответы
    @task()
    def print_data(year_of_interest, best_selling_game_global, most_popular_genres_eu,
                   most_popular_platforms_na, top_publishers_jp, games_sold_better_eu_than_jp):
        print(f'1. Самая продаваемая в {year_of_interest} г. во всем мире игра: {best_selling_game_global}')
        print(f'2. Игры жанра(ов) {most_popular_genres_eu} были самыми продаваемыми в Европе в {year_of_interest} г.')
        print(f'''3. На платформе(ах) {most_popular_platforms_na} было больше всего игр, 
        которые продались более, чем миллионным тиражом в Северной Америке в {year_of_interest} г.''')
        print(f'4. У издателя(ей) {top_publishers_jp} самые высокие средние продажи в Японии в {year_of_interest} г.')
        print(f'5. {games_sold_better_eu_than_jp} игр продались лучше в Европе, чем в Японии в {year_of_interest} г.')

    data = get_data(year_of_interest)
    best_selling_game_global = get_best_selling_game_global(data)
    most_popular_genres_eu = get_most_popular_genres_eu(data)
    most_popular_platforms_na = get_most_popular_platforms_na(data)
    top_publishers_jp = get_top_publishers_jp(data)
    games_sold_better_eu_than_jp = get_games_sold_better_eu_than_jp(data)
    print_data(year_of_interest, best_selling_game_global, most_popular_genres_eu,
               most_popular_platforms_na, top_publishers_jp, games_sold_better_eu_than_jp)

d_morozova_20_games_dag = d_morozova_20_games_dag()
