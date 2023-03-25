import pandas as pd
import telegram

from datetime import timedelta
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

YEAR = 1994 + hash('m_laptev_28') % 23

default_args = {
    'owner': 'm.laptev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 7),
    'schedule_interval': '0 12 * * *'
}

BOT_TOKEN = '5966276665:AAE7OzUxG1i6Y-HkFoJe9cksAziCbHm3hcA'
CHAT_ID = 710871004

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)



@dag(default_args=default_args, catchup=False)
def m_laptev_28_less3dag():

    @task()
    def get_data():
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        df = df.query('Year == @YEAR')
        return df

    @task()
    def get_bs_game(data):
        games_sales = data.groupby('Name') \
            .agg({'Global_Sales': 'sum'}) \
            .reset_index()
        max_sales = games_sales['Global_Sales'].max()
        bs_game_world = games_sales.query('Global_Sales == @max_sales')['Name'].iloc[0]
        return str(bs_game_world)

    @task()
    def get_bs_genre(data):
        bs_genre_eu = data.groupby('Genre') \
            .agg({'EU_Sales': 'sum'}) \
            .reset_index()
        max_ = bs_genre_eu['EU_Sales'].max()
        return list(bs_genre_eu.query('EU_Sales == @max_')['Genre'])

    @task()
    def get_over1M_na(data):
        games_over1M_NA = data.query('NA_Sales > 1') \
            .groupby('Platform') \
            .agg({'Name': 'count'}) \
            .reset_index()
        max_ = games_over1M_NA.Name.max()
        return list(games_over1M_NA.query('Name == @max_')['Platform'])

    @task()
    def get_bestavgsales_jp_publ(data):
        avg_sales_jap_pb = data.groupby('Publisher').agg({'JP_Sales': 'mean'}).sort_values('JP_Sales', ascending=False).reset_index()
        max_ = avg_sales_jap_pb['JP_Sales'].max()
        return list(avg_sales_jap_pb.query('JP_Sales == @max_')['Publisher'])

    @task()
    def get_games_eu_over_jp(data):
        game_sales = data.groupby('Name') \
            .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'}) \
            .reset_index() \
            .query('EU_Sales > JP_Sales')
        return game_sales.shape[0]

    @task(on_success_callback=send_message)
    def print_data(q1, q2, q3, q4, q5):
        context = get_current_context()
        genre_case = 'жанра' if len(q2) == 1 else 'жанров'
        platf_case = 'платформе' if len(q3) == 1 else 'платформах'
        publ_case = 'издателя' if len(q4) == 1 else 'издателей'
        games_case = 'игр' if q5 in (11, 12, 13, 14) else ('игрa' if q5 % 10 == 1 else ('игры' if q5 % 10 in (2, 3, 4) else 'игр'))
        print(f'''
        Самая продаваемая игра в мире в {YEAR} году это: {q1}
        Самыми продаваемыми в Европе в {YEAR} году были игры {genre_case}: {', '.join(q2)}
        Наибольшее количество игр тиражом более 1М в Северной Америке в {YEAR} году было продано на {platf_case}: {', '.join(q3)}
        Самые высокие средние продажи в Японии в {YEAR} году были у {publ_case}: {', '.join(q4)}
        В {YEAR} году в Европе продалось лучше чем в Японии {q5} {games_case}''')

    data = get_data()
    bs_game = get_bs_game(data)
    bs_genre_eu = get_bs_genre(data)
    games_over1M_na = get_over1M_na(data)
    best_avgsales_jp_publ = get_bestavgsales_jp_publ(data)
    sales_eu_over_jp = get_games_eu_over_jp(data)
    print_data(bs_game, bs_genre_eu, games_over1M_na, best_avgsales_jp_publ, sales_eu_over_jp)

m_laptev_28_less3dag = m_laptev_28_less3dag()

