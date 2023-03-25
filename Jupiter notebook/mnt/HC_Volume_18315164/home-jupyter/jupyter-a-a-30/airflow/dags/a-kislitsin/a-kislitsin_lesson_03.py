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


SOURCE_FILE = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'a-kislitsin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2023, 1, 10),
    'schedule_interval': '0 12 * * *'
}

CHAT_ID = -1001830768673#-620798068
try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = ''
    
BOT_TOKEN = '5592898652:AAFxx4t98RqO9AQd-GKXW2Ghv6-hu_4Dwgs'
#определение года
login = 'a-kislitsin'
var_year = 1994+hash(f'{login}')%23
#print(f' год для проведения расчетов ={var_year}')

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

@dag(default_args=default_args, catchup=False)
def a_kislitsin_lesson_3():
    @task(retries=3)
    def get_data():
        source_data = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        return source_data

    @task(retries=4, retry_delay=timedelta(10))
    def get_task_1(source_data):
        print(source_data)
        #самая продаваемая игра в var_year в мире
        #source_data =  pd.read_csv(StringIO(source_data))
        top_game = source_data.query(f"Year == {float(var_year)}").groupby(['Name'], as_index=True).agg({'Global_Sales':'sum'}).sort_values('Global_Sales', ascending=False).reset_index()
        top_game = top_game.head(1)
        #print('Самое продаваемое в мире')
        #print(top_game)
        return top_game.to_csv(index=False)

    @task()
    def get_task_2(source_data):
        #source_data =  pd.read_csv(StringIO(source_data))
        #самая продаваемая игра в var_year в Европе
        top_game_EU = source_data.query(f"Year == {float(var_year)}").groupby(['Genre'], as_index=True).agg({'EU_Sales':'sum'}).sort_values('EU_Sales', ascending=False).reset_index()
        top_game_EU_1 = top_game_EU.head(1)
        #print('Самое продаваемое в европе')
        #print(top_game_EU_1)
        top_game_EU_2 = pd.merge(top_game_EU_1,top_game_EU, on='EU_Sales', how='left')
        #print(top_game_EU_2)
        return top_game_EU_2.to_csv(index=False)  #{'ru_avg': ru_avg, 'ru_median': ru_median}

    @task()
    def get_task_3(source_data):
        #самое большое количество игр проданных более 1 млн. экз. в var_year в Америке
        #source_data =  pd.read_csv(StringIO(source_data))
        top_game_A = source_data.query(f"Year == {float(var_year)}").groupby(['Platform'], as_index=True).agg({'NA_Sales':'sum'}).sort_values('NA_Sales', ascending=False).reset_index()
        #print('Самое продаваемое в америке')
        top_game_A['Upper'] = top_game_A['NA_Sales'] >1.0
        top_game_A = top_game_A.query("Upper ==True")
        top_game_A = top_game_A.drop(columns='Upper') 
        #print(top_game_A)
        #print(top_data_df)
        return top_game_A.to_csv(index=False) 

    @task()
    def get_task_4(source_data):
        #самые высокие средние продажи в var_year в Японии
        #source_data =  pd.read_csv(StringIO(source_data))
        top_game_J = source_data.query(f"Year == {float(var_year)}").groupby(['Platform'], as_index=True).agg({'JP_Sales':'mean'}).sort_values('JP_Sales', ascending=False).reset_index()
        #print('Самое продаваемое в японии')
        #print(top_game_J.head(1))
        return top_game_J.head(1).to_csv(index=False) 

    @task()
    def get_task_5(source_data):
        #разница игр в европе с играми в японии в var_year 
        #source_data =  pd.read_csv(StringIO(source_data))
        count_game_J = source_data.query(f"Year == {float(var_year)}").groupby(['Name'], as_index=True).agg({'JP_Sales':'sum', 'EU_Sales':'sum'}).sort_values('EU_Sales', ascending=False).reset_index()
        count_game_J['D']=count_game_J['EU_Sales']-count_game_J['JP_Sales']
        #count_game_J[count_game_J['D']>0].count()
        #print('количество продаваемое ')
        #print(count_game_J.head())
        #print(count_game_J[count_game_J['D']>0].D.count())
        return count_game_J[count_game_J['D']>0].D.count()



    @task(on_success_callback=send_message)
    def print_data(task1_data, task2_data, task3_data, task4_data, task5_data, var_year):

        context = get_current_context()
        date = context['ds']

        #ru_avg, ru_median = ru_stat['ru_avg'], ru_stat['ru_median']
        #com_avg, com_median = com_stat['com_avg'], com_stat['com_median']

        print(f'''Данные за {date}''')
        print('1.Самое продаваемая игра в мире')
        print(task1_data)
        print('2.Самое продаваемое в европе')
        print(task2_data)
        print('3.Самое большое количество игр проданных более 1 млн. экз. в var_year в Америке')
        print(task3_data)       
        print('4.Самые высокие средние продажи в var_year в Японии')
        print(task4_data)            
        print('5.Самые высокие средние продажи в var_year в Японии')
        print(task5_data) 


    source_data = get_data()

    task1_data = get_task_1(source_data)
    task2_data = get_task_2(source_data)
    task3_data = get_task_3(source_data)
    task4_data = get_task_4(source_data)
    task5_data = get_task_5(source_data)

    print_data(task1_data, task2_data, task3_data, task4_data, task5_data, var_year)

a_kislitsin_lesson_3 = a_kislitsin_lesson_3()
