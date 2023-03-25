from io import StringIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

NAME_FILE = 'vgsales.csv'

default_args = {
    'owner': 's.chobanu',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 29),
    'schedule_interval': '0 12 * * *'
}

# CHAT_ID = -620798068
# try:
#     BOT_TOKEN = Variable.get('telegram_secret')
# except:
#     BOT_TOKEN = ''

# def send_message(context):
#     date = context['ds']
#     dag_id = context['dag'].dag_id
#     message = f'Huge success! Dag {dag_id} completed on {date}'
#     if BOT_TOKEN != '':
#         bot = telegram.Bot(token=BOT_TOKEN)
#         bot.send_message(chat_id=CHAT_ID, text=message)
#     else:
#         pass

@dag(default_args=default_args, catchup=False)
def stat_games_2016_airflow_2():
    @task()#retries=3)
    def get_data():
        df = pd.read_csv(NAME_FILE).decode('utf-8')
        df_2016 = df[df['Year'] == 2016.]
        df_2016['Name'] = df_2016['Name'].astype('str')
        df_2016['Platform'] = df_2016['Platform'].astype('str')
        df_2016['Genre'] = df_2016['Genre'].astype('str')
        df_2016['Publisher'] = df_2016['Publisher'].astype('str')
        return df_2016

    @task()#retries=4, retry_delay=timedelta(10))
    def get_most_sales_game_global(df_2016):
        data = pd.read_csv(StringIO(df_2016))
        most_sales_global = data.groupby('Name', as_index=False)\
                                .agg({'Global_Sales':'sum'})\
                                .sort_values(by='Global_Sales', ascending=False)\
                                .head(1).Name
        
        return most_sales_global.to_csv(index=False)

    @task()
    def get_most_sales_eu_genre(df_2016):
        data = pd.read_csv(StringIO(df_2016))
        most_sales_eu_genre = df_2016.groupby('Genre', as_index=False)\
                                    .agg({'EU_Sales':'sum'})\
                                    .sort_values(by='EU_Sales', ascending=False)\
                                    .head(1).Genre
        return most_sales_eu_genre.to_csv(index=False)

    @task()
    def get_most_count_games_na(df_2016):
        data = pd.read_csv(StringIO(df_2016))
        most_count_games_na = data.query('NA_Sales > 1')\
                .groupby('Platform', as_index=False)\
                .agg({'Name':'nunique'})\
                .sort_values(by='Name', ascending=False)\
                .head(1).Platform
        return most_count_games_na.to_csv(index=False)

    @task()
    def get_mean_sales_games_publ_jp(df_2016):
        data = pd.read_csv(StringIO(df_2016))
        mean_sales_games_publ_jp = data.groupby('Publisher', as_index=False)\
                                        .agg({'JP_Sales':'mean'})\
                                        .sort_values(by='JP_Sales', ascending=False)\
                                        .head(1).Publisher
        return mean_sales_games_publ_jp.to_csv(index=False)
    
    @task()
    def get_count_games_eu_jp(df_2016):
        data = pd.read_csv(StringIO(df_2016))
        count_games_eu_jp = data.query('EU_Sales > JP_Sales').Name.nunique()
        return count_games_eu_jp.to_csv(index=False)


    @task()#on_success_callback=send_message)
    def print_data(most_sales_global, most_sales_eu_genre, most_count_games_na, mean_sales_games_publ_jp, count_games_eu_jp):
        print(f'''Сколько игр продались лучше в Европе, чем в Японии?
                  {most_sales_global}
              ''')
        print(f'''Игры какого жанра были самыми продаваемыми в Европе? 
                  Перечислить все, если их несколько
                  {most_sales_eu_genre}        
               ''')
        print(f'''На какой платформе было больше всего игр, 
                  которые продались более чем миллионным тиражом в Северной Америке?
                  {most_count_games_na}        
               ''')
        print(f'''У какого издателя самые высокие средние продажи в Японии? 
                 Перечислить все, если их несколько.
                 {mean_sales_games_publ_jp}
               ''')       
        print(f'''Сколько игр продались лучше в Европе, чем в Японии?
                 {count_games_eu_jp}        
               ''')
        
        
    data_2016 = get_data()
    
    most_sales_game_global = get_most_sales_game_global(data_2016)
    most_sales_eu_genre = get_most_sales_eu_genre(data_2016)
    most_count_games_na = get_most_count_games_na(data_2016)
    mean_sales_games_publ_jp = get_mean_sales_games_publ_jp(data_2016)
    count_games_eu_jp = get_count_games_eu_jp(data_2016)
    
    print_data(most_sales_game_global, most_sales_eu_genre, most_count_games_na, mean_sales_games_publ_jp, count_games_eu_jp)

stat_games_2016_airflow_2 = stat_games_2016_airflow_2()