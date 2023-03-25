import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


default_args = {
    'owner': 'a-nozdrin',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 30),
    'schedule_interval':'0 10 * * *'
}


def send_message(context):
    CHAT_ID = -604455156
    BOT_TOKEN = "5586827407:AAGfuXHY8lG9mUpkOy-GnHZHlGO2JDi_Ph0"
    date = context['ds']
    dag_id = context['dag'].dag_id
    #date = '111'
    #dag_id = '222'
    message = f'Dag {dag_id} completed on {date}'
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)   

@dag(default_args=default_args, catchup=False)
def a_nozdrin_lesson_3():
    @task()
    def get_data():
        login = 'a-nozdrin'
        year = float(1994 + hash(f'{login}') % 23)
        # path = 'vgsales.csv'
        path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
        df = pd.read_csv(path)
        df_my_year = df.query('Year == @year')
        return df_my_year.to_csv(index=False)

    @task()
    def game_with_max_global_sales(df_my_year_csv):
        df_my_year = pd.read_csv(StringIO(df_my_year_csv))
        game_with_max_global_sales = df_my_year[df_my_year['Global_Sales'] == df_my_year['Global_Sales'].max()].Name.values
        return {'game_with_max_global_sales': game_with_max_global_sales}

    @task()
    def genre_with_max_eu_sales(df_my_year_csv):
        df_my_year = pd.read_csv(StringIO(df_my_year_csv))
        df_my_year_genre_agg = df_my_year.groupby('Genre').agg(EU_Sales_sum = ('EU_Sales','sum'))
        genres = df_my_year_genre_agg[df_my_year_genre_agg['EU_Sales_sum'] == df_my_year_genre_agg['EU_Sales_sum'].max()].index.values
        return {'genre_with_max_eu_sales': genres}
    
    @task()
    def platform_with_max_eu_sales_count_more_1m(df_my_year_csv):
        df_my_year = pd.read_csv(StringIO(df_my_year_csv))
        df_my_year_platform_agg = df_my_year.query('NA_Sales > 1').groupby('Platform').agg(name_count =('Name','count'))
        platform = df_my_year_platform_agg[df_my_year_platform_agg['name_count'] == df_my_year_platform_agg['name_count'].max()].index.values
        return {'platform_with_max_eu_sales_count_more_1m': platform}
    
    @task()
    def publisher_with_max_avg_jp_sales(df_my_year_csv):
        df_my_year = pd.read_csv(StringIO(df_my_year_csv))
        df_my_year_publisher_agg = df_my_year.groupby('Publisher').agg(JP_Sales_mean = ('JP_Sales','mean'))
        publisher = df_my_year_publisher_agg[df_my_year_publisher_agg['JP_Sales_mean'] == df_my_year_publisher_agg['JP_Sales_mean'].max()].index.values
        return {'publisher_with_max_avg_jp_sales': publisher}

    @task()
    def count_eu_sales_more_then_jp_sales(df_my_year_csv):
        df_my_year = pd.read_csv(StringIO(df_my_year_csv))
        count = df_my_year.query('EU_Sales > JP_Sales').shape[0]
        return {'count_eu_sales_more_then_jp_sales': count}

    @task(on_success_callback=send_message)
    def print_data(t1, t2, t3, t4, t5):
        context = get_current_context()
        date = context['ds']
        #date = '111'
        game_with_max_global_sales = t1['game_with_max_global_sales']
        genre_with_max_eu_sales = t2['genre_with_max_eu_sales']
        platform_with_max_eu_sales_count_more_1m = t3['platform_with_max_eu_sales_count_more_1m']
        publisher_with_max_avg_jp_sales = t4['publisher_with_max_avg_jp_sales']
        count_eu_sales_more_then_jp_sales = t5['count_eu_sales_more_then_jp_sales']

        print(f'''Games with Max Global Sales for date {date}
                  {", ".join(game_with_max_global_sales)}''')
        
        print(f'''Genres with Max EU Sales for date {date}
                  {", ".join(genre_with_max_eu_sales)}''')
        
        print(f'''Platforms with Max Count of Games with Global Sales > 1m for date {date}
                  {", ".join(platform_with_max_eu_sales_count_more_1m)}''')
        
        print(f'''Publishers with Max Avg JP Sales for date {date}
                  {", ".join(publisher_with_max_avg_jp_sales)}''')
        
        print(f'''Games Count with EU Sales > JP Sales for date {date}
                  {count_eu_sales_more_then_jp_sales}''')

    df_my_year = get_data()
    
    transform_1_output = game_with_max_global_sales(df_my_year)
    transform_2_output = genre_with_max_eu_sales(df_my_year)
    transform_3_output = platform_with_max_eu_sales_count_more_1m(df_my_year)
    transform_4_output = publisher_with_max_avg_jp_sales(df_my_year)
    transform_5_output = count_eu_sales_more_then_jp_sales(df_my_year)
    
    print_data(transform_1_output, transform_2_output, transform_3_output, transform_4_output, transform_5_output)

a_nozdrin_lesson_3 = a_nozdrin_lesson_3()