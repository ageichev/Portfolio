import pandas as pd
import telegram
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

my_year = 1994 + hash(f'g-genba-23') % 23

default_args = {
    'owner': 'g-genba-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 28),
    'schedule_interval': '0 12 * * *'
}

@dag(default_args=default_args, catchup=False)

def g_genba_23_lesson3():
    
    @task()
    
    # Собираем все данные за 1994 год
    
    def get_all_data():
        all_data = pd.read_csv(vgsales) \
                 .query('Year == @my_year')
        return all_data


    @task()
    
    # Какая игра была самой продаваемой в этом году во всем мире?
    
    def get_bestseller_game(all_data):
        bestseller_game = all_data[all_data.Global_Sales == all_data.Global_Sales.max()] \
                                      .Name.values[0]
        return bestseller_game


    @task()
    
    # Игры какого жанра были самыми продаваемыми в Европе?
    
    def get_top_Europe_Genre(all_data):
        top_Europe_Genre = all_data.groupby('Genre', as_index=False) \
                                   .agg({'EU_Sales': 'sum'})
        top_Europe_Genre = top_Europe_Genre[top_Europe_Genre.EU_Sales == top_Europe_Genre.EU_Sales.max()] \
                                              .Genre.to_list()
        return top_Europe_Genre


    @task()
    
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    
    def get_top_NA_Platform(all_data):
        top_NA_Platform = all_data.query('NA_Sales > 1') \
                                  .groupby('Platform', as_index=False) \
                                  .agg({'Name': 'count'}) \
                                  .rename(columns={'Name': 'Number'})
        top_NA_Platform = top_NA_Platform[top_NA_Platform.Number == top_NA_Platform.Number.max()] \
                                                .Platform.to_list()
        return top_NA_Platform


    @task()
    
    # У какого издателя самые высокие средние продажи в Японии?
    
    def get_top_JP_Publisher(all_data):
        top_JP_Publisher = all_data.groupby('Publisher', as_index=False) \
                                   .agg({'JP_Sales': 'mean'})
        top_JP_Publisher = top_JP_Publisher[top_JP_Publisher.JP_Sales == top_JP_Publisher.JP_Sales.max()] \
                                                            .Publisher.to_list()
        return top_JP_Publisher


    @task
    
    # Сколько игр продались лучше в Европе, чем в Японии?
    
    def get_EU_more_JP(all_data):
        EU_more_JP =  all_data.query('EU_Sales > JP_Sales').shape[0]
        return EU_more_JP

    @task()
    
    # Вывод на печать
    
    def print_data(bestseller_game, top_Europe_Genre, top_NA_Platform, top_JP_Publisher, EU_more_JP):

        context = get_current_context()
        date    = context['ds']

        print(f'''Данные от {my_year} года, информация найдена {date}
                  Какая игра была самой продаваемой в этом году во всем мире? Ответ: {bestseller_game}
                  Игры какого жанра были самыми продаваемыми в Европе? Ответ: {top_Europe_Genre}
                  На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Ответ: {top_NA_Platform}
                  У какого издателя самые высокие средние продажи в Японии?  Ответ: {top_JP_Publisher}
                  Сколько игр продались лучше в Европе, чем в Японии? Ответ: {EU_more_JP}''')


    all_data = get_all_data()

    bestseller_game  = get_bestseller_game(all_data)
    top_Europe_Genre = get_top_Europe_Genre(all_data)
    top_NA_Platform  = get_top_NA_Platform(all_data)
    top_JP_Publisher = get_top_JP_Publisher(all_data)
    EU_more_JP       = get_EU_more_JP(all_data)

    print_data(bestseller_game, top_Europe_Genre, top_NA_Platform, top_JP_Publisher, EU_more_JP)

g_genba_23_lesson3 = g_genba_23_lesson3()