import pandas as pd
from datetime import timedelta
from datetime import datetime
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

df = 'vgsales.csv'
login = 'top_10_ru_ivshin'
year = 1994 + hash(f'{login}') % 24

default_args = {
    'owner': 'm-ivshin-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 10),
    'schedule_interval': '0 8 * * *'
}

CHAT_ID = -5396383168
try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = ''

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
def top_10_airflow_2():
    @task(retries=3)
    def load_data():
        sales_data = pd.read_csv(df)
        sales_data = sales_data.query('Year == @year')
        return sales_data.to_csv(index=False)

    @task(retries=4, retry_delay=timedelta(10))
    def get_top_game(sales_data):
        top_game = pd.read_csv(sales_data)
        index = top_game.Global_Sales.idxmax()
        top_game = top_game.loc[index].Name
        return top_game

    @task()
    def get_top_genre_eu(sales_data):
        top_genre_eu = pd.read_csv(sales_data)
        top_genre_eu = sales_data.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'}) 
        top_genre_eu = top_genre_eu[top_genre_eu.EU_Sales == max(top_genre_eu.EU_Sales)].Genre
        return top_genre_eu

    @task()
    def get_NA_top_platform(sales_data):
        NA_platforms = pd.read_csv(sales_data)
        NA_platforms = NA_platforms[NA_platforms.NA_Sales > 1] \
            .groupby('Platform', as_index=False).agg({'Name': 'count'}) 
        NA_top_platform = NA_platforms[NA_platforms.Name == max(NA_platforms.Name)].Platform
        return NA_top_platform

    @task()
    def get_top_publisher_jp(sales_data):
        top_publisher_jp = pd.read_csv(sales_data)
        top_publisher_jp = top_publisher_jp.groupby('Publisher', as_index=False).agg({'JP_Sales': 'sum'})
        top_publisher_jp = top_publisher_jp[top_publisher_jp.JP_Sales == max(top_publisher_jp.JP_Sales)].Publisher
        return top_publisher_jb
    
    @task()
    def get_eu_vs_jp(sales_data):
        eu_vs_jp = pd.read_csv(sales_data)
        eu_vs_jp = eu_vs_jp[eu_vs_jp.EU_Sales > eu_vs_jp.JP_Sales].shape[0]
        return eu_vs_jp

    @task(on_success_callback=send_message)
    def print_data(year, top_game, top_genre_eu, NA_top_platform, top_publisher_jp, eu_vs_jp):
        
        login = 'top_10_ru_ivshin'
        year = 1994 + hash(f'{login}') % 24
    
        print(f'''Data for year {year}\n 
                  Top game: {top_game}.
                  Top genre in EU: {top_genre_eu}.
                  Top platform in NA: {NA_top_platform}.
                  Top publisher in Japan: {top_publisher_jp}.
                  There are {eu_vs_jp} games which are sold better in the EU than in Japan.
                  ''')


    sales_data = load_data()
    top_game = get_top_game(sales_data)
    top_genre_eu = get_top_genre_eu(sales_data)
    NA_top_platform = get_NA_top_platform(sales_data)
    top_publisher_jp = get_top_publisher_jp(sales_data)
    eu_vs_jp = get_eu_vs_jp(sales_data)
    print_data()

top_10_airflow_2 = top_10_airflow_2()