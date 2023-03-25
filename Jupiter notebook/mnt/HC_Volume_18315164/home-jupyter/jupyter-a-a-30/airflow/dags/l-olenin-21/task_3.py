import pandas as pd
from datetime import timedelta
from datetime import datetime

import telegram
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

VGSALES = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

CHAT_ID = '188021753'
BOT_TOKEN = Variable.get('telegram_secret')

login = 'l-olenin-21'
year = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'l-olenin-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 1, 7, 45),
    'schedule_interval': '@daily'
}

@dag(default_args=default_args, catchup=False)
def l_olenin_21_task_3():

    @task
    def get_data():
        data = pd.read_csv(VGSALES)
        return data.query('Year == @year')

    @task
    # 1. Какая игра была самой продаваемой в этом году во всем мире?
    def get_max_global_sales(data):
        max_sales = data.loc[data.Global_Sales.idxmax()]
        return {'name': max_sales.Name,
                'sales': max_sales.Global_Sales}

    @task
    # 2. Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    def get_eu_genre_top(data):
        df = data.groupby('Genre', as_index=False)['EU_Sales'].sum()
        max_sales = df.EU_Sales.max()
        return df.query('EU_Sales == @max_sales')

    @task
    # 3. На какой платформе было больше всего игр, которые продались более
    #    чем миллионным тиражом в Северной Америке?
    def get_million_platform(data):
        df = data.query('NA_Sales > 1') \
            .groupby('Platform', as_index=False) \
            .size() \
            .rename(columns={'size': 'games_count'})

        max_sales = df.loc[df.games_count.idxmax()]
        return {'platform': max_sales.Platform,
                'count': max_sales.games_count}

    @task
    # 4. У какого издателя самые высокие средние продажи в Японии?
    def get_publisher_jp(data):
        df = data.groupby('Publisher', as_index=False)['JP_Sales'].mean()
        max_sales = df.loc[df.JP_Sales.idxmax()]
        return {'publisher': max_sales.Publisher,
                'mean_sales': max_sales.JP_Sales}

    @task
    # 5. Сколько игр продались лучше в Европе, чем в Японии?
    def get_sales_eu_over_jp(data):
        df = data[data['EU_Sales'] > data['JP_Sales']]
        return {'count': len(df)}

    @task(on_success_callback = send_message)
    def print_data(max_global_sales, eu_genre_top, million_platform,
                   publisher_jp, sales_eu_over_jp):

        context = get_current_context()
        print(f'For the state at {context["ds"]}')
        print(f'In the {year} year:')

        # 1. Какая игра была самой продаваемой в этом году во всем мире?
        print(f'1. The best global game is {max_global_sales["name"]} with {round(max_global_sales["sales"],2)} million sales')

        # 2. Игры какого жанра были самыми продаваемыми в Европе?
        #    Перечислить все, если их несколько
        if len(eu_genre_top) == 1:
            first_pos = eu_genre_top.iloc[0]
            print(f'2. The best genre in Europe is {first_pos.Genre} with {round(first_pos.EU_Sales,2)} million sales')
        else:
            print(f'2. The best genres in Europe is:')
            print(eu_genre_top)

        # 3. На какой платформе было больше всего игр, которые продались более
        #    чем миллионным тиражом в Северной Америке?
        print(f'3. The best platform in North America is {million_platform["platform"]} with {million_platform["count"]} sales over million')

        # 4. У какого издателя самые высокие средние продажи в Японии?
        print(f'4. The best publisher in Japan is {publisher_jp["publisher"]} with {round(publisher_jp["mean_sales"],2)} mean sales')

        # 5. Сколько игр продались лучше в Европе, чем в Японии?
        print(f'5. {sales_eu_over_jp["count"]} games were sold more in Europe than in Japan')

    data = get_data()
    max_global_sales    = get_max_global_sales(data)
    eu_genre_top        = get_eu_genre_top(data)
    million_platform    = get_million_platform(data)
    publisher_jp        = get_publisher_jp(data)
    sales_eu_over_jp    = get_sales_eu_over_jp(data)

    print_data(max_global_sales, eu_genre_top, million_platform,
               publisher_jp, sales_eu_over_jp)


def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id

    message = f'Dag {dag_id} is completed on {date}'

    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)

l_olenin_21_task_3 = l_olenin_21_task_3()



