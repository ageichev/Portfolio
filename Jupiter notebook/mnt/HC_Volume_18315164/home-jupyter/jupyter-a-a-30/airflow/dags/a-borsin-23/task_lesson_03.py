import pandas as pd
from datetime import timedelta
from datetime import datetime
import telegram

from airflow.decorators import dag, task
from airflow.models import Variable

default_args = {
    'owner': 'a.borsin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 30),
    'schedule_interval': '0 12 * * *'
}

vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'a-borsin-23'
year = 1994 + hash(f'{login}') % 23

CHAT_ID = 126870144
BOT_TOKEN = '5613132007:AAGeDMWCQbi6VSlwz5vU3sipH8hOi7dLAX4'


def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)


@dag(default_args=default_args, catchup=False)
def vgsales_task():

    @task()
    def get_data(year):
        df = pd.read_csv(vgsales)
        df = df.query('Year == @year')
        df = df.reset_index().drop(columns='index')
        return df

    @task()
    def top_global_sales(df):
        best_selling_game = df.sort_values(by='Global_Sales', ascending=False).head(1).Name[0]
        return best_selling_game

    @task()
    def top_eu_genre(df):
        best_selling_genre_eu = df.groupby('Genre', as_index=False) \
            .agg({'EU_Sales': 'sum'}) \
            .sort_values(by='EU_Sales', ascending=False) \
            .head(1) \
            .Genre.values[0]
        return best_selling_genre_eu

    @task()
    def top_na_platform(df):
        platform = df.query('NA_Sales > 1.0') \
            .groupby('Platform', as_index=False) \
            .agg({'Name': 'count'}) \
            .rename(columns={'Name': 'number'}) \
            .sort_values(by='number', ascending=False) \
            .head(1) \
            .Platform.values[0]
        return platform

    @task()
    def top_jp_mean_publisher(df):
        top_jp_publisher = df.groupby('Publisher', as_index=False) \
            .agg({'JP_Sales': 'mean'}) \
            .sort_values(by='JP_Sales', ascending=False) \
            .head(1) \
            .Publisher.values[0]
        return top_jp_publisher

    @task()
    def eu_jp(df):
        num_games = df.query('EU_Sales > JP_Sales').Name.unique().shape[0]
        return num_games

    @task(on_success_callback=send_message)
    def print_data(top_game, eu_genre, na_platform,
                   jp_publisher, eu_jp):
        print(f'Данные представленны за {year}')
        print(f'Самой продоваемой игрой стала {top_game}')
        print(f'Самый продаваемый жанр в Европе {eu_genre}')
        print(f'Больше всего игр, боллее чем миллионным тиражом, в Северной Америки продавалось на {na_platform}')
        print(f'Самые высокие средние продажи в Японии у {jp_publisher}')
        print(f'{eu_jp} продались лучше в Европе, чем в Японии')

    df = get_data(year)
    top_game = top_global_sales(df)
    eu_genre = top_eu_genre(df)
    na_platform = top_na_platform(df)
    jp_publisher = top_jp_mean_publisher(df)
    eu_jp = eu_jp(df)

    print_data(top_game, eu_genre, na_platform, jp_publisher, eu_jp)

vgsales_task = vgsales_task()