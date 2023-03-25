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

# задание входных данных:путь к файлу, логин, вычисление года 
input_data = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'g-zigora-23'
year = 1994 + hash(f'login') % 23

default_args = {
    'owner': 'g-zigora-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 17),
    'schedule_interval': '0 10 * * *'
}

# доступ в тг
CHAT_ID = 
try:
    BOT_TOKEN = ''
except:
    BOT_TOKEN = ''

#смс в телеграм
def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} with data for {year} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

@dag(default_args=default_args, catchup=False)
def airflow_3():
    @task()
    def get_data_games():
        data_in = pd.read_csv(input_data).query('Year == @year')
        return data_in
# самая продаваемая игра
    @task()
    def get_top_sale(data_in):
        top_sale = data_in.groupby('Name').sum() \
                        .nlargest(1,"Global_Sales").index[0]
        return {'top_sale': top_sale}

# самый продаваемый жанр игр в Европе
    @task()
    def get_top_eu_genre(data_in):
        top_eu_genre = data_in.groupby('Genre').sum() \
                            .nlargest(1,"EU_Sales").index[0]
        return {'top_eu_genre': top_eu_genre}

# игры, проданные более чем милионным тиражом в Cеверной Fмерике    
    @task()
    def get_sale_1M_na(data_in):
        sale_1M_na = data_in.query('NA_Sales > 1') \
                            .groupby('Platform') \
                            .count().reset_index()
        sale_1M_na = sale_1M_na[sale_1M_na['Rank'] == sale_1M_na['Rank'] \
                                .max()].Platform.unique()[0]
        return {'sale_1M_na': sale_1M_na}

#самые высокие продажи в Японии
    @task()
    def get_top_pub_jp(data_in):
        top_pub_jp = data_in.groupby('Publisher') \
                            .mean().reset_index()
        top_pub_jp = top_pub_jp[top_pub_jp['JP_Sales'] == top_pub_jp['JP_Sales'] \
                            .max()].Publisher.unique()[0]
        return {'top_pub_jp': top_pub_jp}

#сколько игр продались в Европе лучше, чем в японии
    @task()
    def get_sale_eu_jp(data_in):
        sale_eu_jp = data_in.groupby('Name').sum().reset_index()
        sale_eu_jp = sale_eu_jp[sale_eu_jp['EU_Sales']>sale_eu_jp['JP_Sales']] \
                            .Name.count()
        return {'sale_eu_jp': sale_eu_jp}
#  результаты
    @task(on_success_callback=send_message)
    def print_data(t1, t2, t3, t4, t5):

        context = get_current_context()
        date = context['ds']

        top_sale, top_eu_genre  = t1['top_sale'], t2['top_eu_genre']
        top_pub_jp, sale_1M_na = t4['top_pub_jp'], t3['sale_1M_na']
        sale_eu_jp = t5['sale_eu_jp']
        print(f'''Информация за {year} год на {date}
                  Какая игра была самой продаваемой в этом году во всем мире?: {top_sale}
                  Игры какого жанра были самыми продаваемыми в Европе?: {top_eu_genre}
                  На какой платформе было больше всего игр, проданных более чем миллионным тиражом в NA: {sale_1M_na}
                  У какого издателя самые высокие средние продажи в Японии?: {top_pub_jp}
                  Сколько игр продались лучше в Европе, чем в Японии?: {sale_eu_jp}''')

        
    data_in = get_data_games()
    t1 = get_top_sale(data_in)
    t2 = get_top_eu_genre(data_in)
    t3 = get_sale_1M_na(data_in)
    t4 = get_top_pub_jp(data_in)
    t5 = get_sale_eu_jp(data_in)
    print_data(t1, t2, t3, t4, t5)

airflow_3 = airflow_3()



