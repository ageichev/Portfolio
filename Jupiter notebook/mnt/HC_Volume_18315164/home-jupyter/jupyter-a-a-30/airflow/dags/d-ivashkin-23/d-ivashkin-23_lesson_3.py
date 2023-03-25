import pandas as pd
from datetime import timedelta
from datetime import datetime
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

vgsales_data = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'd-ivashkin-23'
year = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'd-ivashkin-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 24),
    'schedule_interval': '25 17 * * *'
}

CHAT_ID = 164410810
try:
    BOT_TOKEN = '5340980291:AAF-Okj2bMwxLGzOqlrT9Jb6DiNSuEO7fFI'
except:
    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Haha, I have done this, Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

@dag(default_args=default_args, catchup=False)
def d_ivashkin_23_lesson_3():

#Получаем датафрейм:

    @task(retries=3)
    def get_games_data():
        games_data = pd.read_csv(vgsales_data).query('Year == @year')
        return games_data

#Какая игра была самой продаваемой в этом году во всем мире?

    @task(retries=4, retry_delay=timedelta(10))
    def get_top_game_ww(games_data):
        top_game_ww = games_data.Name.iloc[0]
        return top_game_ww

#Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько

    @task()
    def get_top_genre_eu(games_data):
        top_genre_eu = games_data \
            .groupby('Genre', as_index=False) \
            .agg({'EU_Sales': 'sum'}) \
            .sort_values('EU_Sales', ascending=False).Genre.iloc[0]
        return top_genre_eu

#На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?

    @task()
    def get_top_platf_na(games_data):
        top_platf_na = games_data \
            .query('NA_Sales>1') \
            .groupby('Platform', as_index=False) \
            .agg({'Name':'count'}) \
            .sort_values('Name', ascending=False) \
            .Platform \
            .iloc[0]
        return top_platf_na

#У какого издателя самые высокие средние продажи в Японии?

    @task()
    def get_top_publisher_jp(games_data):
        top_publisher_jp = games_data \
            .groupby('Publisher', as_index=False) \
            .agg({'JP_Sales':'mean'}) \
            .sort_values('JP_Sales', ascending=False) \
            .Publisher \
            .iloc[0]
        return top_publisher_jp
#Сколько игр продались лучше в Европе, чем в Японии?

    @task()
    def get_eu_games_over_jp(games_data):
        eu_games_over_jp = games_data \
            .query('EU_Sales > JP_Sales') \
            .shape[0]
        return {'eu_games_over_jp': eu_games_over_jp}

    @task(on_success_callback=send_message)
    def print_data(top_game_ww, top_genre_eu, top_platf_na, top_publisher_jp, eu_games_over_jp):

        context = get_current_context()
        date = context['ds']

        print(f'''Data for {date}
                  The best-selling game in {year} worldwide was {top_game_ww}
                  The best-selling game genre in Europe in {year} was {top_genre_eu}
                  Platform with the most games with over a million sales in North America in {year} was {top_platf_na}
                  Publisher with the highest average sales in Japan in {year} was {top_publisher_jp}
                  Number of games that sold better in Europe than in Japan in {year} - {eu_games_over_jp}
            ''')

    games_data = get_games_data()
    top_game_ww = get_top_game_ww(games_data)
    top_genre_eu = get_top_genre_eu(games_data)
    top_platf_na = get_top_platf_na(games_data)
    top_publisher_jp = get_top_publisher_jp(games_data)
    eu_games_over_jp = get_eu_games_over_jp(games_data)

    print_data(top_game_ww, top_genre_eu, top_platf_na, top_publisher_jp, eu_games_over_jp)


d_ivashkin_23_lesson_3 = d_ivashkin_23_lesson_3()
