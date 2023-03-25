from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable
import telegram

import pandas as pd
from datetime import timedelta
from datetime import datetime

year  = 1994 + hash('v-smorodnikova-24') % 23

default_args = {
    'owner': 'v.smorodnikova',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 10, 29),
    'schedule_interval': '30 10 * * *'
}

CHAT_ID = 1172870783
BOT_TOKEN = '5403343786:AAFFX08GZzHEBjR2g3pWS5b1kWObg4Bvbkw'

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=1172870783, text=message)

@dag(default_args = default_args, catchup = False)
def v_smorodnikova_24_games_analyse():
    
    @task()
    def get_data():
        data = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        data = data[data.Year == year]
        return data

    @task()
    def top_world(data):
        top_world = data.groupby('Name').agg({'Global_Sales': 'sum'}).sort_values('Global_Sales', ascending = False).index[0]
        return top_world
    
    @task()
    def top_eu(data):
        eu = data.groupby('Genre').agg({'EU_Sales': 'sum'}).sort_values('EU_Sales', ascending = False).round()
        max_sales = eu.EU_Sales.max()
        top_eu = list(eu[eu.EU_Sales == max_sales].index)
        return top_eu
        
    @task()
    def top_platforms_na(data):
        platforms_na = data[data.NA_Sales > 1].groupby('Platform').agg({'Name': 'nunique'}).sort_values('Name', ascending = False)
        max_quantity = platforms_na.Name.max()
        top_platforms_na = list(platforms_na[platforms_na.Name == max_quantity].index)
        return top_platforms_na
    
    @task()
    def top_publishers_jp(data):
        publishers_jp = data.groupby('Publisher').agg({'JP_Sales':'mean'}).sort_values('JP_Sales', ascending = False).round(2)
        max_average_sales = publishers_jp.JP_Sales.max()
        top_publishers_jp  = list(publishers_jp[publishers_jp.JP_Sales == max_average_sales].index)
        return top_publishers_jp
    
    @task()
    def comparison(data):
        games_quantity = data.groupby('Name').agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'}).query('EU_Sales > JP_Sales').EU_Sales.count()
        return games_quantity
    
    @task(on_success_callback=send_message)
    def print_results(task1, task2, task3, task4, task5, year):
        
        context = get_current_context()
        date = context['ds']
        
        print(f'Cамой продаваемой во всем мире в {year} году была игра {task1}' '\n'
            f'Игры жанра {", ".join(task2)} были самыми продаваемыми в Европе в {year} году' '\n'
            f'На платформе {", ".join(task3)} было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке в {year} году' '\n'
            f'У издателя {", ".join(task4)} были самые высокие средние продажи в Японии в {year} году' '\n'
            f'{task5} игр продались лучше в Европе, чем в Японии в {year} году')

    data = get_data()
    
    top_world = top_world(data)
    top_eu = top_eu(data)
    top_platforms_na = top_platforms_na(data)
    top_publishers_jp = top_publishers_jp(data)
    games_quantity = comparison(data)
        
    print_results(top_world, top_eu, top_platforms_na, top_publishers_jp, games_quantity, year)
    
v_smorodnikova_24_games_analyse = v_smorodnikova_24_games_analyse()    


    
    
    
    
    
    
    
    
    
    
    
    
    
    