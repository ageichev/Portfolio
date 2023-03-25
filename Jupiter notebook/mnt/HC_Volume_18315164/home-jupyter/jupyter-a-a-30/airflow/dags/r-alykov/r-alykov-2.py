#import requests
#from zipfile import ZipFile
#from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
#from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


# df = pd.read_csv('vgsales.csv')

# df = df.query('Year == 2003')


default_args = {
    'owner': 'r.alykov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 10)
}
schedule_interval = '*/5 * */1 * *'

@dag(default_args=default_args, schedule_interval = schedule_interval, catchup=False)

def ralykov_airflow_2():
    
    @task(retries=3)
    def get_data():
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        df = df.query('Year == 2003')
        return df

# Какая игра была самой продаваемой в этом году во всем мире?
    @task(retries=3)
    def most_sold_game(df):
        df_most = df\
            .groupby('Name', as_index=False)\
            .agg({'Global_Sales': 'max'})\
            .sort_values('Global_Sales', ascending=False).head(1)
        with open('df_most.csv', 'w') as f:
            f.write(df_most.to_csv(index=False, header=False))
            f.close()
    
# Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task(retries=3)
    def eu_sales(df):
        df_EU = df[['EU_Sales', 'Genre']]
        df_EU = df_EU.groupby('Genre', as_index=False)\
            .agg({'EU_Sales': 'sum'})\
            .sort_values('EU_Sales', ascending=False).head(5)
        with open('df_EU.csv', 'w') as f:
            f.write(df_EU.to_csv(index=False, header=False))
            f.close()

# На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
# Перечислить все, если их несколько
    @task(retries=3)
    def na_sales(df):
        df_na = df\
            .query('NA_Sales > 1.00')\
            .groupby('Platform', as_index=False)\
            .agg({'NA_Sales': 'count'})\
            .sort_values('NA_Sales', ascending=False)
        with open('df_na.csv', 'w') as f:
            f.write(df_na.to_csv(index=False, header=False))
            f.close()

# У какого издателя самые высокие средние продажи в Японии?
# Перечислить все, если их несколько
    @task(retries=3)
    def jp_sales(df):
        df_jp = df[['Publisher', 'JP_Sales']]
        df_jp = df_jp\
            .groupby('Publisher', as_index=False)\
            .agg({'JP_Sales': 'mean'})\
            .sort_values('JP_Sales', ascending=False).head(5)
        with open('df_jp.csv', 'w') as f:
            f.write(df_jp.to_csv(index=False, header=False))
            f.close()
    
# Сколько игр продались лучше в Европе, чем в Японии?
    @task(retries=3)
    def jp_eu_sales_diff(df):
        df_jp_vs_eu = df[['Name', 'JP_Sales', 'EU_Sales']]
        df_jp_vs_eu = df_jp_vs_eu\
            .query('EU_Sales > JP_Sales')\
            .value_counts('Name', ascending=False).count()
        df_jp_vs_eu = str(df_jp_vs_eu)
        with open('df_jp_vs_eu.txt', 'w') as f:
            f.write(df_jp_vs_eu)
            f.close()
            
    @task(retries=3)
    def print_data(most_sold_game, eu_sales, na_sales, jp_sales, jp_eu_sales_diff):

        with open('df_most.csv', 'r') as f:
            most_sold_game_data = f.read()
        with open('df_EU.csv', 'r') as f:
            eu_sales_data = f.read()
        with open('df_na.csv', 'r') as f:
            na_sales_data = f.read()
        with open('df_jp.csv', 'r') as f:
            jp_sales_data = f.read()
        with open('df_jp_vs_eu.txt', 'r') as f:
            jp_eu_sales_diff_data = f.read()

        print('Most sold game in 2003:')
        print(most_sold_game_data)

        print('Most sold genre in EU in 2003: ')
        print(eu_sales_data)
    
        print('Million sold copies in Northen America in 2003:')
        print(na_sales_data)

        print('Highest avg solds by publisher in Japan in 2003:')
        print(jp_sales_data)
        
        print('the number of games that were sold in Europe better than in Japan шт 2003:')
        print(jp_eu_sales_diff_data)
        
    df = get_data()
    most_sold_game = most_sold_game(df)
    eu_sales = eu_sales(df)
    na_sales = na_sales(df)
    jp_sales = jp_sales(df)
    jp_eu_sales_diff = jp_eu_sales_diff(df)

    print_data(most_sold_game, eu_sales, na_sales, jp_sales, jp_eu_sales_diff)

ralykov_airflow_2 = ralykov_airflow_2()