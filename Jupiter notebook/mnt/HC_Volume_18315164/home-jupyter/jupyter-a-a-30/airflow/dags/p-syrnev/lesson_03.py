import pandas as pd
import numpy as np
from io import StringIO
from datetime import timedelta
from datetime import datetime
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


login = 'p-syrnev'
target_year = 1994 + hash(f'{login}') % 23
# path = 'vgsales.csv'
path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

CHAT_ID = 351301198
BOT_TOKEN = '5407072426:AAFCawYX4BjbHDTdbsO9gKDlCScW5cgJhSM'

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Ура! Dag {dag_id} завершился {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

default_args = {
    'owner': 'p-syrnev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 22),
    'schedule_interval' : '0 10 * * *',
}

@dag(default_args=default_args,
     catchup=False,
     tags=['p-syrnev-23','lesson-03'])
def p_syrnev_23_lesson_03():

    @task()
    def get_target_year_data():
        # загрузим и отфильтруем датасет по заданному году
        df_target_year = pd.read_csv(path).query('Year == @target_year')
        df_target_year.Year = df_target_year.Year.astype('int') 
        return df_target_year.to_csv(index=False)

    @task()
    def get_best_selling_game(target_year_data : str):
        # Какая игра была самой продаваемой в этом году во всем мире?
        df = pd.read_csv(StringIO(target_year_data))

        return df.groupby(by='Name', as_index=False) \
            .agg({'Global_Sales':'sum'}) \
            .rename(columns={'Global_Sales':'Total_Sales'}) \
            .sort_values(by='Total_Sales', ascending=False) \
            .iloc[0].Name
    
    @task()
    def get_best_selling_games_eu(target_year_data : str):
        # Игры какого жанра были самыми продаваемыми в Европе?
        # Перечислить все, если их несколько
        df = pd.read_csv(StringIO(target_year_data))

        df_genre_eu_sales = df.groupby(by='Genre', as_index=False).agg({'EU_Sales':'sum'})

        max_sales = df_genre_eu_sales.EU_Sales.max()

        return ', '.join(df_genre_eu_sales.query('EU_Sales == @max_sales').Genre.tolist())

    @task()
    def get_1m_platform_na(target_year_data : str):
        # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
        # Перечислить все, если их несколько
        df = pd.read_csv(StringIO(target_year_data))

        df['IsGreatThan1M'] = df.NA_Sales > 1.0

        df = df.groupby(by=['Platform'], as_index=False) \
            .agg({'IsGreatThan1M':'sum'}) \
            .rename(columns={'IsGreatThan1M':'Count_games'})

        max_1m = df.Count_games.max()

        return ', '.join(df.query('Count_games == @max_1m').Platform.tolist())

    @task()
    def get_publisher_with_max_avegrage_jp(target_year_data : str):
        # У какого издателя самые высокие средние продажи в Японии?
        # Перечислить все, если их несколько
        df = pd.read_csv(StringIO(target_year_data))

        df_avg_jp = df.groupby(by='Publisher', as_index=False) \
                    .agg({'JP_Sales':'mean'})

        max_avg_jp = df_avg_jp.JP_Sales.max()

        return ', '.join(df_avg_jp.query('JP_Sales == @max_avg_jp').Publisher.tolist())

    @task()
    def get_count_eu_games(target_year_data : str):
        # Сколько игр продались лучше в Европе, чем в Японии?
        df = pd.read_csv(StringIO(target_year_data))

        df_game_eu_vs_jp =df.groupby(by='Name') \
            .agg({'EU_Sales':'sum', 'JP_Sales' : 'sum'})

        return (df_game_eu_vs_jp.EU_Sales > df_game_eu_vs_jp.JP_Sales).sum()

    @task(on_success_callback=send_message)
    def print_result(best_selling_game,
                    best_selling_games_eu,
                    platform_with_1m,
                    publisher_with_max_avegrage_jp,
                    eu_vs_jp):
        
        context = get_current_context()

        date = context['ds']
        print(f'''Даг запущен: {date}

        Будем смотреть данные за {target_year}г.

        1. Какая игра была самой продаваемой в этом году во всем мире?
        *{best_selling_game}*

        2. Игры какого жанра были самыми продаваемыми в Европе?
        {best_selling_games_eu}

        3. На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
        {platform_with_1m}

        4. У какого издателя самые высокие средние продажи в Японии?
        {publisher_with_max_avegrage_jp}
        
        5. Сколько игр продались лучше в Европе, чем в Японии?
        {eu_vs_jp}
        ''')

    target_year_data = get_target_year_data()

    best_selling_game = get_best_selling_game(target_year_data)
    best_selling_games_eu = get_best_selling_games_eu(target_year_data)
    platform_with_1m = get_1m_platform_na(target_year_data)
    publisher_with_max_avegrage_jp = get_publisher_with_max_avegrage_jp(target_year_data)
    eu_vs_jp = get_count_eu_games(target_year_data)

    print_result(best_selling_game,
                 best_selling_games_eu,
                 platform_with_1m,
                 publisher_with_max_avegrage_jp,
                 eu_vs_jp)
    
# run dag
dag_p_syrnev_23_lesson_03 = p_syrnev_23_lesson_03()
    