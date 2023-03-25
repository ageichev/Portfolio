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

df_file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
default_args = {
    'owner': 'a.pozolotina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 28)
    
}

@dag(default_args=default_args, schedule_interval='0 12 * * *', catchup=False)
def games_data_lesson_3_by_pozolotina():

    #1 task
    @task()
    def get_data():
        df = pd.read_csv(df_file) #прочитали файл
        #выбор года - выбираю 2009 с самым большим количеством игр
        year = float(df.query('Year>1994').groupby('Year', as_index = False).agg({'Name': 'count'})\
        .sort_values('Name', ascending = False).head(1).iloc[:,0])
        t1_df_2009 = df[df.Year == year]
        return t1_df_2009

    #2 task
    @task()
    def data_task2(t1_df_2009):
        #1.Какая игра была самой продаваемой в этом году во всем мире?
        #вывод строки с максимальным знаечнием по Global_Sales
        t2 = t1_df_2009.loc[t1_df_2009.Global_Sales == t1_df_2009.Global_Sales.max()]
        t2 = t2.Name
        return t2
    
    #3 task
    @task()
    def data_task3(t1_df_2009):
        #2. Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
        #вывод строк - это общая сумма продаж по жанрам - больше 90% максимальной суммы по жанру
        t3 = t1_df_2009.groupby('Genre', as_index = False).agg({'EU_Sales': 'sum'}).sort_values('EU_Sales', ascending = False)
        t3 = (t3.loc[t3.EU_Sales > (t3.EU_Sales.max()*0.9)]).Genre
        return t3
    
    #4 task
    @task()
    def data_task4(t1_df_2009):
        #3. На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? 
        #Перечислить все, если их несколько
        #отфильтровали игры с тиражом больше 1 млн, сгруппировали по платформе, 
        #вывод строки/строк больше 90% от максимального количества игр по агрегированному столбцу
        t4 = t1_df_2009.query('NA_Sales > 1').groupby('Platform', as_index = False).agg({'Name': 'count'})\
        .sort_values('Name', ascending = False)
        t4 = (t4.loc[t4.Name > (t4.Name.max()*0.9)]).Platform
        return t4

    #5 task
    @task()
    def data_task5(t1_df_2009):
        #4. У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
        #группировка по издателю, рассчет средних продаж в Японии - вывод строк больше 90% от максимального значения
        t5 = t1_df_2009.groupby('Publisher', as_index = False).agg({'JP_Sales': 'mean'})\
        .sort_values('JP_Sales', ascending = False)
        t5 = (t5.loc[t5.JP_Sales > (t5.JP_Sales.max()*0.9)]).Publisher
        return t5

    #6 task
    @task()
    def data_task6(t1_df_2009):
        #5. Сколько игр продались лучше в Европе, чем в Японии?
        #выводит строки, где EU_Sales больше, чем JP_Sales
        t6 = t1_df_2009.loc[t1_df_2009.EU_Sales > t1_df_2009.JP_Sales]
        t6 = len(t6.axes[0])
        return t6
     
    #7 task
    @task()
    def print_data(t2, t3, t4, t5, t6):

        context = get_current_context()
        date = context['ds']

        print(f'The best selling game in the world - {t2} by {date}')
        
        print(f'The best selling genre in Europe - {t3} by {date}')
        
        print(f'The platform where circulation games > 1 million in North America - {t4} by {date}')
        
        print(f'The best avg selling publisher in Japan - {t5} by {date}')

        print(f'The number of games that sold better in Europe than in Japan - {t6} by {date}')

    #зависимости
    t1_df_2009 = get_data()#файл на выходе = функция по его получению
    t2 = data_task2(t1_df_2009)#файл на выходе следующего таска = функция этого таска + на вход файл с данными
    t3 = data_task3(t1_df_2009)
    t4 = data_task4(t1_df_2009)
    t5 = data_task5(t1_df_2009)
    t6 = data_task6(t1_df_2009)
    
    print_data(t2, t3, t4, t5, t6)#запуск функции вывода результатов
    

airflow_3_by_pozolotina = games_data_lesson_3_by_pozolotina() #имя дага и его функция