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



year = 1994 + hash(f'l-kovach-23') % 23

path_to_file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

CHAT_ID = 5110351847
try:
    BOT_TOKEN = '5259887486:AAEp2zc-UzLPyAxHgKepZkKShRmb_iBoxGc'
except:
    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = (f'Успешно! Даг {dag_id} завершился {date}')
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

default_args = {
    'owner': 'l.kovach',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 20),
    'schedule_interval' : '0 13 * * *',
}

@dag(default_args=default_args, catchup=False)
def lidiakovach_03():

    @task()
#     Сначала определим год, за какой будем смотреть данные (скачиваем и фильтруем дф).
    def get_year():
        df_year = pd.read_csv(path_to_file).query('Year == @year')
        df_year.Year = df_year.Year.astype('int') 
        return df_year

    @task()
#      1.Какая игра была самой продаваемой в этом году во всем мире?
    def get_best_selling(df_year):
        df_best_selling = df_year
        df_best_selling = df_best_selling.groupby('Name', as_index=False) \
            .agg({'Global_Sales':'sum'}) \
            .sort_values('Global_Sales', ascending=False) \
            .iloc[0].Name
        return df_best_selling
    
    @task()
#     2.Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    def get_best_selling_europe(df_year):
        df_best_selling_europe = df_year.groupby('Genre').agg({'EU_Sales':'sum'}).sort_values('EU_Sales', ascending=False)
        df_best_selling_europe = df_best_selling_europe.EU_Sales.idxmax()
        return df_best_selling_europe


    @task()
#     3.На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
      # Перечислить все, если их несколько
    def get_platform_na(df_year):
        get_platform_na = df_year.query("NA_Sales > 1.0").groupby('Platform').agg({'NA_Sales' : 'sum'}).NA_Sales.idxmax()
        return get_platform_na

    @task()
#     4.У какого издателя самые высокие средние продажи в Японии?
# Перечислить все, если их несколько
    def get_avg_sales_JP(df_year):
        get_avg_sales_JP = df_year.groupby('Publisher').agg({'JP_Sales' : 'mean'}).JP_Sales.idxmax()
        return get_avg_sales_JP

    @task()
#    5. Сколько игр продались лучше в Европе, чем в Японии?
    def get_eu_vs_jp(df_year):
        get_eu_vs_jp_count = df_year
        get_eu = get_eu_vs_jp_count.groupby('Name', as_index=False).agg({'EU_Sales':'sum'})
        get_jp = get_eu_vs_jp_count.groupby('Name', as_index=False).agg({'JP_Sales' : 'sum'})
        get_eu_vs_jp = get_eu.merge(get_jp, on='Name')
        get_eu_vs_jp = get_eu_vs_jp.query('EU_Sales > JP_Sales').shape[0]
        return get_eu_vs_jp


    @task(on_success_callback=send_message)
    def print_data(best_selling,
                 best_selling_europe,
                 platform_na,
                 avg_sales_JP,
                 eu_vs_jp):
        
        context = get_current_context()

        date = context['ds']
        print(f'Даг на: {date}')
        print(f'Смотрим данные за {year} год')
        print(f'1. Самая продаваемая игра в {year} году во всем мире была {best_selling}')
        print(f'2. Самые продаваемые игры в Европе были игры жанра {best_selling_europe}')
        print(f'3. Больше всего игр, которые продались более чем миллионным тиражом в Северной Америке, было на платформе {platform_na}')
        print(f'4. Самые высокие средние продажи в Японии у издателя {avg_sales_JP}')
        print(f'5. {eu_vs_jp} игр продалось в Европе лучше, чем в Японии')

       


    df_year = get_year()
    best_selling = get_best_selling(df_year)
    best_selling_europe = get_best_selling_europe(df_year)
    platform_na = get_platform_na(df_year)
    avg_sales_JP = get_avg_sales_JP(df_year)
    eu_vs_jp = get_eu_vs_jp(df_year)

    print_data(best_selling,
                 best_selling_europe,
                 platform_na,
                 avg_sales_JP,
                 eu_vs_jp)
    

lidiakovach_03 = lidiakovach_03()