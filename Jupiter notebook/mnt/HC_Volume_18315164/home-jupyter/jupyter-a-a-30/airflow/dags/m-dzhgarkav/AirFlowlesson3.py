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


login = 'm-dzhgarkav'
year = 1994 + hash('{login}') % 23
print(year)


default_args = {
                'owner': 'm-dzhgarkav',
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2022, 10, 15),
                'schedule_interval': '0 12 * * *'
                }



CHAT_ID = '1153368296'
BOT_TOKEN = '5403470983:AAFewAqQN0RBXlSF7s8nqdgGit5LYPX-dUE'
    

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)

    
    

@dag(default_args=default_args, catchup=False)
def m_dzhgarkav_airflow_3():
    @task()
    def get_data():
        path_to_file = r'/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
        vgsales = pd.read_csv(path_to_file, sep=',', index_col=False)
        vgsales = vgsales[vgsales['Year'] == year]
        return vgsales

    @task()
    def top_games(vgsales):
        # самоя продаваемая игра в этом году во всем мире
        top_game = vgsales.groupby(['Name']).agg({'Global_Sales':'sum'}).idxmax()
        return top_game

    @task()
    def top_sales_eu(vgsales):
        # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
        topGenre = vgsales.groupby(['Genre']).agg({'EU_Sales':'sum'}).idxmax()
        top_game_sales_eu = vgsales.query("Genre in @topGenre")['Name'].unique()
        return top_game_sales_eu

    @task()
    def top_platfom_na(vgsales):
        # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
        topPlatform = vgsales[vgsales['NA_Sales'] > 1].groupby(['Platform']).agg({'NA_Sales':'sum'}).idxmax()
        top_platfom_sales_na = vgsales.query("Platform in @topPlatform")['Name'].unique()
        return top_platfom_sales_na

    @task()
    def top_publisher_jp(vgsales):
        # У какого издателя самые высокие средние продажи в Японии?
        topPublisher = vgsales.groupby(['Publisher']).agg({'JP_Sales':'mean'}).idxmax()
        return topPublisher

    @task()
    def jp_vs_eu(vgsales):
        # Сколько игр продались лучше в Европе, чем в Японии?
        sale_games = vgsales.groupby(['Name']).agg({'JP_Sales':'sum','EU_Sales':'sum'})
        jp_eu = sale_games.query('EU_Sales > JP_Sales').shape[0]
        return jp_eu

    @task(on_success_callback=send_message)
    def print_data(top_game, top_game_sales_eu, top_platfom_sales_na, topPublisher, jp_eu):
        context = get_current_context()
        date = context['ds']
        
        
        print(                         {date}                                        )
        print('---------------------------------------------------------------------')
        print()
        
        print(f'The best selling game in {year} in the world{top_game}')
        
        print()
        print('-------------------------------------------------------------------------')
        print()

        print(f'The best selling genre in {year} in EU {top_game_sales_eu}')
        
        print()
        print('-------------------------------------------------------------------------')
        print()

        print(f'The best selling game platform in {year}  in NA {top_platfom_sales_na}')
        
        print()
        print('-------------------------------------------------------------------------')
        print()

        print(f'The best selling game publisher in {year} in JP {topPublisher}')
        
        print()
        print('-------------------------------------------------------------------------')
        print()

        print(f'The number of games sold in {year} in EU is more than in JP{jp_eu}')
        
        print()
        print('-------------------------------------------------------------------------')
        print()


    vgsales = get_data()
    top_game = top_games(vgsales)
    top_game_sales_eu = top_sales_eu(vgsales)
    top_platfom_sales_na = top_platfom_na(vgsales)
    topPublisher = top_publisher_jp(vgsales)
    jp_eu = jp_vs_eu(vgsales)
    print_data(top_game, top_game_sales_eu, top_platfom_sales_na, topPublisher, jp_eu)



m_dzhgarkav_airflow_3 = m_dzhgarkav_airflow_3()