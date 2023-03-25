import pandas as pd
from datetime import timedelta
from datetime import datetime
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

default_args = {
    'owner': 'e.muraveva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 27),
    'schedule_interval': '0 12 * * *'
}
login = 'e-muraveva'
year = 1994 + hash(f'{login}') % 23

CHAT_ID = -836635055
BOT_TOKEN = '5563867994:AAH0GECrTfyzrW8s3Qq4wzhTHCN076jANS4'

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)


@dag(default_args=default_args)
def e_muraveva_dag_2():
    # Считываем и фильтруем данные
    @task()
    def get_data():
        data = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        filtered_data = data[data['Year'] == year]
        return filtered_data

    # Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def get_top_game(filtered_data):
        top_game = filtered_data.groupby('Name', as_index=False) \
                                .agg({'Global_Sales': 'sum'})
        top_game = top_game[top_game.Global_Sales == top_game.Global_Sales.max()].Name.tolist()
        return top_game

    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def get_top_genre_eu(filtered_data):
        top_genre_eu = filtered_data.groupby('Genre', as_index=False) \
                                    .agg({'EU_Sales': 'sum'})
        top_genre_eu = top_genre_eu[top_genre_eu.EU_Sales == top_genre_eu.EU_Sales.max()].Genre.tolist()
        return top_genre_eu

    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько
    @task()
    def get_top_platform_na(filtered_data):
        top_platform_na = filtered_data.query('NA_Sales > 1') \
                                       .groupby('Platform', as_index=False) \
                                       .agg({'Name': 'nunique'})
        top_platform_na = top_platform_na[top_platform_na.Name == top_platform_na.Name.max()].Platform.tolist()
        return top_platform_na

    # У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def get_top_publisher_jp(filtered_data):
        top_publisher_jp = filtered_data.groupby('Publisher', as_index=False) \
                                        .agg({'JP_Sales': 'mean'})
        top_publisher_jp = top_publisher_jp[top_publisher_jp.JP_Sales == top_publisher_jp.JP_Sales.max()].Publisher.tolist()
        return top_publisher_jp

    # Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def get_top_eu_than_jp(filtered_data):
        top_eu_than_jp = filtered_data.groupby('Name', as_index=False) \
                                      .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})
        top_eu_than_jp = top_eu_than_jp[top_eu_than_jp.EU_Sales > top_eu_than_jp.JP_Sales].shape[0]
        return top_eu_than_jp

    # Вывод
    @task(on_success_callback=send_message)
    def print_data(top_game, top_genre_eu, top_platform_na, top_publisher_jp, top_eu_than_jp):
        context = get_current_context()
        date = context['ds']

        print(f'''Data for {year} for {date}
                  Top Global Game: {top_game}
                  Top EU Genres: {top_genre_eu}
                  Top NA Platforms: {top_platform_na}
                  Top JP Publishers: {top_publisher_jp}
                  The number of games with more sales in the EU than in JP: {top_eu_than_jp}''')

    data = get_data()

    top_game = get_top_game(data)
    top_genre_eu = get_top_genre_eu(data)
    top_platform_na = get_top_platform_na(data)
    top_publisher_jp = get_top_publisher_jp(data)
    top_eu_than_jp = get_top_eu_than_jp(data)

    print_data(top_game, top_genre_eu, top_platform_na, top_publisher_jp, top_eu_than_jp)

e_muraveva_dag_2 = e_muraveva_dag_2()