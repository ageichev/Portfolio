import pandas as pd
from datetime import timedelta
from datetime import datetime
#import telegram

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context

login = 'v-semenjuk-23'
vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 1994 + hash(f'v-semenjuk-23') % 23  #2009


default_args = {
    'owner': 'v-semenjuk-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 6),
    'schedule_interval': '0 12 * * *'
}

@dag(default_args=default_args, catchup=False)
def vgsales_semenjuk():

    @task()
    def get_data():
        data_games = pd.read_csv(vgsales).query("Year == @year")
        return data_games

# Какая игра была самой продаваемой в этом году во всем мире? 

    @task()
    def best_game(data_games):
        best_selling_game = data_games.sort_values('Global_Sales', ascending=False).iloc[0]['Name']
        return best_selling_game

# Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько

    @task()
    def best_genre(data_games):
        best_genre_europe = data_games.groupby('Genre', as_index=False) \
            .agg({'EU_Sales': 'sum'}) \
            .sort_values(by='EU_Sales', ascending=False) \
            .head(1) \
            .Genre.values[0]
        return best_genre_europe

# На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько

    @task()
    def best_platform(data_games):
        best_platform_NA = data_games.query('NA_Sales > 1.0') \
            .groupby('Platform', as_index=False) \
            .agg({'Rank': 'count'}) \
            .sort_values('Rank', ascending=False) \
            .head(1) \
            .Platform.values[0]
        return best_platform_NA
    
# У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько

    @task()
    def best_publisher(data_games):
        best_mean_sl_publisher = data_games.groupby('Publisher', as_index=False) \
            .agg({'JP_Sales': 'mean'}) \
            .sort_values(by='JP_Sales', ascending=False) \
            .head(1) \
            .Publisher.values[0]
        return best_mean_sl_publisher

# Сколько игр продались лучше в Европе, чем в Японии?

    @task()
    def game_europe(data_games):
        selling = data_games.groupby('Name', as_index=False).agg({'EU_Sales':'sum','JP_Sales':'sum'})
        count_best_game_europe = selling[selling.EU_Sales > selling.JP_Sales].shape[0]
        return count_best_game_europe
    
# Запись в лог всех ответов    
    @task()
    def print_data(best_selling_game, best_genre_europe, best_platform_NA, best_mean_sl_publisher, count_best_game_europe):
        print(f'Данные предоставленны за {year}')
        print(f'Самая продаваемая игра в мире {best_selling_game}')
        print(f'Самый продаваемый жанр в Европе {best_genre_europe}')
        print(f'Больше всего игр, боллее чем миллионным тиражом, в Северной Америки продавалось на {best_platform_NA}')
        print(f'Самые высокие средние продажи издателя в Японии у {best_mean_sl_publisher}')
        print(f'Количество игр, которые проданы в Европе лучше, чем в Японии - {count_best_game_europe}')
        
    data_games = get_data()
    best_selling_game = best_game(data_games)
    best_genre_europe = best_genre(data_games)
    best_platform_NA = best_platform(data_games)
    best_mean_sl_publisher = best_publisher(data_games)
    count_best_game_europe = game_europe(data_games)

    print_data(best_selling_game, best_genre_europe, best_platform_NA, best_mean_sl_publisher, count_best_game_europe)

vgsales_semenjuk = vgsales_semenjuk()
