# импортируем библиотеки
import pandas as pd

from datetime import timedelta
from datetime import datetime

import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

# подгружаем данные
sales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year_k = 1994 + hash(f'm-kurilova') % 23

# создаем коды для тасков
default_args = {
    'owner': 'm-kurilova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 24),
    'schedule_interval': '0 0 * * *'
}

# создаем функцию для отправки сообщения в telegram
CHAT_ID = 49432558
try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

@dag(default_args=default_args, catchup=False)
def m_kurilova_lesson_3():
    @task()
    # формируем базу данных по заданному периоду
    def get_data():
        data = pd.read_csv('vgsales.csv').query('Year == @year_k')
        return data

    @task()
    # определяем самую продаваемую игру во всем мире в заданный период
    def get_top_name(data):
        game = data.groupby('Name').agg({'Global_Sales': 'sum'}).reset_index()
        top_name = game.Name[game.Global_Sales == game.Global_Sales.max()]
        top_name = top_name.to_string(index=False)
        return top_name

    @task()
    # определяем жанр или жанры, игры которых были самыми продаваемыми в Европе
    def get_Genre_EU_Sales(data):
        Genre_EU_Sales = data.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'})
        Genre_EU_Sales = Genre_EU_Sales[Genre_EU_Sales.EU_Sales == Genre_EU_Sales.EU_Sales.max()].Genre.to_list()
        return Genre_EU_Sales

    @task()
    # определяем платформу(мы) с наибольшим количеством игр с более чем миллионым тиражом в Северной Америке
    def get_Platform_NA_Sales(data):
        Platform_NA_Sales = data[data.NA_Sales > 1].groupby('Platform', as_index=False).agg({'NA_Sales': 'count'})
        Platform_NA_Sales = Platform_NA_Sales[Platform_NA_Sales.NA_Sales == Platform_NA_Sales.NA_Sales.max()]\
                                   .Platform.to_list()
        return Platform_NA_Sales

    @task()
    # определяем издателя с самыми высокими средними продажами в Японии
    def get_Publ_JP_Sales(data):
        Publ_JP_Sales = data.groupby('Publisher', as_index=False).agg({'JP_Sales': 'median'})
        Publ_JP_Sales = Publ_JP_Sales[Publ_JP_Sales.JP_Sales == Publ_JP_Sales.JP_Sales.max()].Publisher.to_list()
        return Publ_JP_Sales

    @task()
    # определяем количество игр, продаваемых в Европе лучше, чем в Японии
    def get_EU_JP_Sales(data):
        EU_Sales = data.groupby('Name', as_index=False).agg({'EU_Sales': 'sum'})
        JP_Sales = data.groupby('Name', as_index=False).agg({'JP_Sales': 'sum'})
        EU_JP_Sales = EU_Sales.merge(JP_Sales, on='Name')
        EU_JP_Sales = EU_JP_Sales.query('EU_Sales > JP_Sales').shape[0]
        return EU_JP_Sales


    @task(on_success_callback=send_message)
    def print_data(top_name, Genre_EU_Sales, Platform_NA_Sales, Publ_JP_Sales, EU_JP_Sales):
        
        context = get_current_context()
        date = context['ds']

        print(f'''Info for {year_k} collected on {date}
                  TOP Game: {top_name}
                  TOP Genre in EU: {Genre_EU_Sales}
                  TOP Platform of 1 mln plus games in NA: {Platform_NA_Sales}
                  TOP Publisher in JP: {Publ_JP_Sales}
                  TOP Selling Games EU&JP: {EU_JP_Sales}''')


    data = get_data()

    top_name = get_top_name(data)
    Genre_EU_Sales = get_Genre_EU_Sales(data)
    Platform_NA_Sales = get_Platform_NA_Sales(data)
    Publ_JP_Sales = get_Publ_JP_Sales(data)
    EU_JP_Sales = get_EU_JP_Sales(data)

    print_data(top_name, Genre_EU_Sales, Platform_NA_Sales, Publ_JP_Sales, EU_JP_Sales)

m_kurilova_lesson_3 = m_kurilova_lesson_3()