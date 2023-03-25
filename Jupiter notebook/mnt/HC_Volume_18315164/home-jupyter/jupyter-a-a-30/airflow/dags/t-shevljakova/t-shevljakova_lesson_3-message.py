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

data_vgsale='/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login='t-shevljakova'
year=1994 + hash(f'{login}') % 23

default_args = {
    'owner': 't-shevljakova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 19),
}
schedule_interval = '0 8 * * *'

CHAT_ID = 796036485
try:
    BOT_TOKEN = '5328246617:AAEiXJjQ6sdw1ciXDzM58ryE1TRBkLn1QM0'
except:
    BOT_TOKEN = ''

@dag(default_args=default_args, catchup=False, schedule_interval=schedule_interval)
def t_shevljakova_lesson3_m():
    @task(retries=3)
    def get_data():
        df_vgsale = pd.read_csv(data_vgsale).query('Year==@year')
        vgsale_data = df_vgsale.to_csv(index=False)
        return vgsale_data

    @task(retries=4, retry_delay=timedelta(10))
    def get_first_top_global(vgsale_data):
        df_vgsale = pd.read_csv(StringIO(vgsale_data))
        first_top_global=df_vgsale.query('Global_Sales==@df_vgsale.Global_Sales.max()').Name.values
        return first_top_global

    @task(retries=4, retry_delay=timedelta(10))
    def get_first_top_EU(vgsale_data):
        df_vgsale = pd.read_csv(StringIO(vgsale_data))
        first_top_EU=df_vgsale.query('EU_Sales==@df_vgsale.EU_Sales.max()').Name.values
        return first_top_EU
    
    @task(retries=4, retry_delay=timedelta(10))
    def get_game_on_platform_max(vgsale_data):
        df_vgsale = pd.read_csv(StringIO(vgsale_data))
        game_on_platform=df_vgsale.query('NA_Sales>1')\
                            .groupby('Platform', as_index=False)\
                            .agg({'Name':'count'})
        game_on_platform_max=game_on_platform.query('Name==@game_on_platform.Name.max()').Platform.values
        return game_on_platform_max
    
    @task(retries=4, retry_delay=timedelta(10))
    def get_publisher_avg_sale_max(vgsale_data):
        df_vgsale = pd.read_csv(StringIO(vgsale_data))
        publisher_avg_sale=df_vgsale.groupby('Publisher', as_index=False)\
                            .agg({'JP_Sales':'mean'})
        publisher_avg_sale_max=publisher_avg_sale.query('JP_Sales==@publisher_avg_sale.JP_Sales.max()').Publisher.values
        return publisher_avg_sale_max
    
    @task(retries=4, retry_delay=timedelta(10))
    def get_count_EU_more_JP(vgsale_data):
        df_vgsale = pd.read_csv(StringIO(vgsale_data))
        count_EU_more_JP=str(df_vgsale.query('EU_Sales>JP_Sales').shape[0])
        return count_EU_more_JP
    
    @task()
    def print_data(first_top_global, first_top_EU, game_on_platform_max, publisher_avg_sale_max, count_EU_more_JP):
               
        print(f'''Самая продаваемая игра в {year} г. в мире: {first_top_global}''')

        print(f'''Самая продаваемая игра в Европе в {year} г. жанра: {first_top_EU}''')
        
        print(f'''Больше всего игр продано тиражом более 1 млн в {year} г. выпущены на платформе: {game_on_platform_max}''')
        
        print(f'''Самые высокие продажи в японии в {year} г. у издателя: {publisher_avg_sale_max}''')
        
        print(f'''Количество игр которые продавались в Европе лучше чем в Японии в {year} г. составило: {count_EU_more_JP}''')

        result = f'''1. Самая продаваемая игра в {year} г. в мире: {first_top_global}.
                     2. Самая продаваемая игра в Европе в {year} г. жанра: {first_top_EU}.
                     3. Больше всего игр продано тиражом более 1 млн в {year} г. выпущены на платформе: {game_on_platform_max}.
                     4. Cамые высокие продажи в японии в {year} г. у издателя: {publisher_avg_sale_max}.
                     5. Количество игр которые продавались в Европе лучше чем в Японии в {year} г. составило: {count_EU_more_JP}.'''
        return result
    
    @task(retries=4, retry_delay=timedelta(10))
    def send_message(result):
        context = get_current_context()
        date = context['ds']
        dag_id = context['dag'].dag_id
        message = f'Dag {dag_id} успешен на дату {date}, результат {result}'
        if BOT_TOKEN != '':
            bot = telegram.Bot(token=BOT_TOKEN)
            bot.send_message(chat_id=CHAT_ID, message=message)
        else:
            pass
    
    
    vgsale_data = get_data()
    
    first_top_global = get_first_top_global(vgsale_data)
    first_top_EU = get_first_top_EU(vgsale_data)
    game_on_platform_max=get_game_on_platform_max(vgsale_data)
    publisher_avg_sale_max=get_publisher_avg_sale_max(vgsale_data)
    count_EU_more_JP=get_count_EU_more_JP(vgsale_data)
    result=print_data(first_top_global, first_top_EU, game_on_platform_max, publisher_avg_sale_max, count_EU_more_JP)
    send_message(result)
    
t_shevljakova_lesson3_m= t_shevljakova_lesson3_m()