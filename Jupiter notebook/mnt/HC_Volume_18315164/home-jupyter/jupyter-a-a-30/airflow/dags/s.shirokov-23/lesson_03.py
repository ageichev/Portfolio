import random
import pendulum
import telegram
import pandas as pd
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


default_args = {
    'owner': 's.shirokov-23',
    'depends_on_past': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=10),
    'start_date': pendulum.datetime(2022, 9, 1, tz='Europe/Moscow'),
    'end_date': pendulum.datetime(2022, 12, 31, tz='Europe/Moscow')
    }

CHAT_ID = -1001719388563
BOT_TOKEN = '2019436498:AAHRb8g2ZBBvbm42IRnM1OZ2Dl75ejenj9E'

# try:
#     BOT_TOKEN = Variable.get('2019436498:AAHRb8g2ZBBvbm42IRnM1OZ2Dl75ejenj9E')
# except:
#     BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'DAG {dag_id} выполнен успешно {date}!'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

@dag(default_args=default_args, schedule_interval='0 12 * * *', catchup=False, tags=['airflow', 's.shirokov-23', 'lesson_03'])
def sshirokov_23_lesson_03_dag():
    
    @task(retries=7, retry_delay=timedelta(5))
    def get_data():
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        df.dropna(inplace=True)
        df.Year = df.Year.astype('Int64')
        # year = 1994 + hash(f's.shirokov-23') % 23
        year = random.choice(df.Year.unique())
        data = df.query('Year == @year')
        return data

    @task()
    def get_year(data):
        year = data.Year.values[0]
        return year

    # Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def get_top_game(data):
        top_game = data.nlargest(1, 'Global_Sales').Name.values[0]
        return top_game
    
    # Игры какого жанра были самыми продаваемыми в Европе?
    @task()
    def get_top_genre(data):
        top_genre = data.groupby('Genre', as_index=False) \
                        .agg({'EU_Sales': 'sum'}) \
                        .nlargest(1, 'EU_Sales', keep='all') \
                        .Genre.to_list()
        return top_genre
    
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    @task()
    def get_top_platform_in_NA(data):
        top_platform = data.query('NA_Sales > 1') \
                           .Platform.value_counts() \
                           .to_frame(name='Games') \
                           .nlargest(1, 'Games', keep='all') \
                           .index.to_list()
        return top_platform
    
    # У какого издателя самые высокие средние продажи в Японии?
    @task()
    def get_top_publisher_in_JP(data):
        top_publisher = data.groupby('Publisher', as_index=False) \
                            .agg(Avg_Sales=('JP_Sales', 'mean')) \
                            .nlargest(1, 'Avg_Sales', keep='all') \
                            .Publisher.to_list()
        return top_publisher
        
    # Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def get_diff_sales_EU_vs_JP(data):
        diff_sales = data.groupby('Name') \
                         .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'}) \
                         .query('EU_Sales > JP_Sales').shape[0]
        return diff_sales
    
    @task(on_success_callback=send_message)
    def print_data(top_game, top_genre, top_platform, top_publisher, diff_sales, year):

        contex = get_current_context()
        date = contex['ds']
        
        print(f'Результат расчета для {year} года. Дата формирования расчета {date}')
        
        print(f'Cамой продаваемой игрой в {year} году во всем мире была {top_game}.')
        
        print(f'Cамыми продаваемыми в Европе в {year} году были игры жанра {", ".join(top_genre)}.')
        
        print(f'Среди игр, проданных более чем миллионным тиражом в Северной Америке, в {year} году, больше всего игр было на платформе {", ".join(top_platform)}.')
        
        print(f'Cамые высокие средние продажи в Японии в {year} году у издателя {", ".join(top_publisher)}.')
        
        print(f'В {year} году {diff_sales} игр продались лучше в Европе, чем в Японии.')
        
    data = get_data()
    year = get_year(data)
    top_game = get_top_game(data)
    top_genre = get_top_genre(data)
    top_platform = get_top_platform_in_NA(data)
    top_publisher = get_top_publisher_in_JP(data)
    diff_sales = get_diff_sales_EU_vs_JP(data)
    print_data(top_game, top_genre, top_platform, top_publisher, diff_sales, year)
    
dag_lesson_03 = sshirokov_23_lesson_03_dag()
