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
    'owner': 'a.ageichev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 3)
    
}

@dag(default_args=default_args, schedule_interval='0 12 * * *', catchup=False)
def games_data_by_ageichev():

    @task()
    def get_data():
        df = pd.read_csv(df_file)
        year = float(df.query('Year>1994').groupby('Year', as_index = False).agg({'Name': 'count'})\
        .sort_values('Name', ascending = False).head(1).iloc[:,0])
        t1_df_2009 = df[df.Year == year]
        return t1_df_2009

    @task()
    def data_task2(t1_df_2009):
        t2 = t1_df_2009.loc[t1_df_2009.Global_Sales == t1_df_2009.Global_Sales.max()]
        t2 = t2.Name
        return t2
    
    @task()
    def data_task3(t1_df_2009):
        t3 = t1_df_2009.groupby('Genre', as_index = False).agg({'EU_Sales': 'sum'}).sort_values('EU_Sales', ascending = False)
        t3 = (t3.loc[t3.EU_Sales > (t3.EU_Sales.max()*0.9)]).Genre
        return t3
    
    @task()
    def data_task4(t1_df_2009):
        t4 = t1_df_2009.query('NA_Sales > 1').groupby('Platform', as_index = False).agg({'Name': 'count'})\
        .sort_values('Name', ascending = False)
        t4 = (t4.loc[t4.Name > (t4.Name.max()*0.9)]).Platform
        return t4

    @task()
    def data_task5(t1_df_2009):
        t5 = t1_df_2009.groupby('Publisher', as_index = False).agg({'JP_Sales': 'mean'})\
        .sort_values('JP_Sales', ascending = False)
        t5 = (t5.loc[t5.JP_Sales > (t5.JP_Sales.max()*0.9)]).Publisher
        return t5

    @task()
    def data_task6(t1_df_2009):
        t6 = t1_df_2009.loc[t1_df_2009.EU_Sales > t1_df_2009.JP_Sales]
        t6 = len(t6.axes[0])
        return t6
     
    @task()
    def print_data(t2, t3, t4, t5, t6):

        context = get_current_context()
        date = context['ds']

        print(f'The best selling game in the world - {t2} by {date}')
        
        print(f'The best selling genre in Europe - {t3} by {date}')
        
        print(f'The platform where circulation games > 1 million in North America - {t4} by {date}')
        
        print(f'The best avg selling publisher in Japan - {t5} by {date}')

        print(f'The number of games that sold better in Europe than in Japan - {t6} by {date}')

    t1_df_2009 = get_data()
    t2 = data_task2(t1_df_2009)
    t3 = data_task3(t1_df_2009)
    t4 = data_task4(t1_df_2009)
    t5 = data_task5(t1_df_2009)
    t6 = data_task6(t1_df_2009)
    
    print_data(t2, t3, t4, t5, t6)
    

airflow_3_by_ageichev = games_data_by_ageichev()