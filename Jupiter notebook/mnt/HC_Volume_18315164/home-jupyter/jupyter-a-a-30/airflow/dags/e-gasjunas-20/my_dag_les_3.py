import pandas as pd
from datetime import timedelta
from datetime import datetime
import telegram


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

login = 'e-gasjunas-20'
year = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'e-gasjunas-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 5),
    'schedule_interval': '0 12 * * *'
}

CHAT_ID = 211120707
BOT_TOKEN = '5109779719:AAG-JhN5eUI7mnoUWHeInctAGI3L1Xyitak'


def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Successfull! Dag {dag_id} completed on {date}'
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)


@dag(default_args=default_args, catchup=False)
def e_gasjunas_20_les_3():
    @task()
    def get_data():
        path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
        df = pd.read_csv(path)
        return df

    @task()
    def get_best_sale_game_world(df): #лучшие продажи по названию в мире за 2014 год
        best_sale_game_world = df.query('Year == @year') \
                              .groupby('Name', as_index = False) \
                              .agg({'Global_Sales' : 'sum'}) \
                              .sort_values('Global_Sales', ascending = False) \
                              .iloc[0, 0]
        return best_sale_game_world

    @task()
    def get_best_genre_game_eu(df): #лучшие продажи по жанру в Европе за 2014 год
        best_genre_game_eu = df.query('Year == @year') \
                              .groupby('Genre', as_index = False) \
                              .agg({'EU_Sales' : 'sum'}) \
                              .sort_values('EU_Sales', ascending = False) \
                              .iloc[0, 0]
        return best_genre_game_eu

    @task()
    def get_best_platform_sale_na(df): #лучшие продажи по платформе в Северной Америке
        best_platform_sale_na = df.query('Year == @year and NA_Sales > 1') \
                                .groupby('Platform', as_index = False) \
                                .agg({'Name': 'count'}) \
                                .sort_values('Platform', ascending = False) \
                                .iloc[0, 0]
        return best_platform_sale_na
    
    @task()
    def get_best_mean_sale_jp(df): #лучшие средние продвжи по издателю в Японии
        best_mean_sale_jp = df.query('Year == @year') \
                            .groupby('Publisher', as_index = False) \
                            .agg({'JP_Sales': 'mean'}) \
                            .sort_values('JP_Sales', ascending = False) \
                            .iloc[0, 0]
        return best_mean_sale_jp
    
    @task()
    def get_count_games_eu_jp(df): #кол-во игр, проданных в Европе, лучше чем в Японии 
        count_games_eu_jp = df.query('Year == @year') \
                            .groupby('Name', as_index = False) \
                            .agg({'EU_Sales': 'mean', 'JP_Sales': 'mean'}) \
                            .query('EU_Sales > JP_Sales') \
                            .shape[0]
        return count_games_eu_jp

    #@task()
    @task(on_success_callback=send_message)
    def print_data(best_sale_game_world,
                  best_genre_game_eu,
                  best_platform_sale_na,
                  best_mean_sale_jp,
                  count_games_eu_jp):
#         context = get_current_context()
#         date = context['ds']

        print(f'Best sale game in World for year {year}: {best_sale_game_world}')

        print(f'Best genre game in EU for year {year}: {best_genre_game_eu}')
        
        print(f'Best platform sale in NA for year {year}: {best_platform_sale_na}')
        
        print(f'Best mean sale in JP for year {year}: {best_mean_sale_jp}')
        
        print(f'Games sold better in Europe than in Japan for year {year}: {count_games_eu_jp}')

        
    df = get_data()
    
    best_sale_game_world = get_best_sale_game_world(df)
    best_genre_game_eu = get_best_genre_game_eu(df)
    best_platform_sale_na = get_best_platform_sale_na(df)
    best_mean_sale_jp = get_best_mean_sale_jp(df)
    count_games_eu_jp = get_count_games_eu_jp(df)
    
    print_data(best_sale_game_world,
                  best_genre_game_eu,
                  best_platform_sale_na,
                  best_mean_sale_jp,
                  count_games_eu_jp)
    
e_gasjunas_20_les_3 = e_gasjunas_20_les_3()
    
