import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

DATA_PATH = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'v-hudokormov-21'
YEAR = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'v.khudokormov',
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 29),
}
schedule_interval = '0 11 */1 * *'

CHAT_ID = -654531812
BOT_TOKEN = '5455556317:AAHkhZK7gOD1a9_N_2b57PLQ7rDdC088ExQ'

def send_message(context):
    import requests
    import json
    from urllib.parse import urlencode

    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    params = {'chat_id': CHAT_ID, 'text': message}
    base_url = f'https://api.telegram.org/bot{BOT_TOKEN}/'
    url = base_url + 'sendMessage?' + urlencode(params)
    resp = requests.get(url)

@dag(default_args=default_args, catchup =False)
def v_khudokormov_lesson_3_hw():
    @task
    def get_prepare_data():
        import pandas as pd
        data = pd.read_csv(DATA_PATH)
        # rename the columns to the lower case
        new_columns = {}
        for column_name in data.columns:
            new_columns[column_name] = column_name.lower()
        data = data.rename(columns=new_columns)
        # filter the dataset to contain only the data for required year
        data = data[data.year == YEAR]
        return data

    @task
    def most_selling_game(data):
        most_selling_game_df = data.groupby('name', as_index=False) \
            .agg({'global_sales': 'sum'}) \
            .sort_values('global_sales', ascending=False) \
            .head(1)
        most_selling_game = most_selling_game_df.name.to_list()[0]
        return most_selling_game

    @task
    def most_selling_genres_eu(data):
        most_selling_genres_eu = data \
            .groupby('genre', as_index=False) \
            .agg({'eu_sales': 'sum'}) \
            .sort_values('eu_sales', ascending=False)
        most_selling_genres_eu_list = most_selling_genres_eu[
            most_selling_genres_eu.eu_sales == most_selling_genres_eu.eu_sales.max()] \
            .genre.to_list()
        return most_selling_genres_eu_list

    @task
    def platforms_w_1M_sells(data):
        platforms_w_1M_sells = data \
            .query("na_sales > 1") \
            .groupby('platform', as_index=False) \
            .agg({'name': 'count'}) \
            .sort_values('name', ascending=False) \
            .platform.to_list()
        return platforms_w_1M_sells

    @task
    def biggest_sales_jp(data):
        biggest_sales_jp = data[data.jp_sales == data.jp_sales.max()].publisher.to_list()
        return biggest_sales_jp

    @task
    def sales_eu_better_jp(data):
        # Некоторые игры выходили на нескольких платформах. Если считать только уникальные имена игр,
        # то их будет столько:
        game_unique_names_count = data[data.eu_sales > data.jp_sales].name.nunique()
        # Если же считать количество игр без учета платформ, то количетсов игр будет следующим:
        game_names_count = data[data.eu_sales > data.jp_sales].shape[0]
        games_list = [game_unique_names_count, game_names_count]
        return games_list

    @task(on_success_callback=send_message)
    def print_output(YEAR, most_selling_game, most_selling_genres_eu_list, platforms_w_1M_sells, biggest_sales_jp, games_list):
        print(f'The most selling game in {YEAR} was {most_selling_game}')
        print('***********')
        print(f'The most selling genres in EU in {YEAR} was {most_selling_genres_eu_list}')
        print('***********')
        print(f'The platofrm(s), which have the most number of more than 1M sells in NA in {YEAR} is {platforms_w_1M_sells}')
        print('***********')
        print(f'The Japan publisher {biggest_sales_jp} has the highest sells in {YEAR}')
        print('***********')
        print(f'{games_list[0]} of unique games sold in Europe better than in Japan in {YEAR}')
        print(f'and considering the platform, in total, there are {games_list[1]}  of games')

    data = get_prepare_data()
    most_selling_game = most_selling_game(data)
    most_selling_genres_eu_list = most_selling_genres_eu(data)
    platforms_w_1M_sells = platforms_w_1M_sells(data)
    biggest_sales_jp = biggest_sales_jp(data)
    games_list = sales_eu_better_jp(data)

    print_output(YEAR, most_selling_game, most_selling_genres_eu_list, platforms_w_1M_sells, biggest_sales_jp, games_list)

v_khudokormov_lesson_3_hw = v_khudokormov_lesson_3_hw()