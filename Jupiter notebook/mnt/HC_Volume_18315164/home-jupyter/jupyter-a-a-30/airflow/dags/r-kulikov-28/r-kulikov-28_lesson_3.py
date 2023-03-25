import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow.decorators import dag, task

TOP_1M_DOMAINS = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
TOP_1M_DOMAINS_FILE = 'vgsales.csv'

default_args = {
    'owner': 'rkulikov28',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 15)
}


@dag(default_args=default_args, schedule_interval='0 12 * * *', catchup=False)
def r_kulikov_28_lesson_3():
    @task()
    def get_data_r_kulikov_28():
        path = requests.get(TOP_1M_DOMAINS, stream=True)
        data = pd.read_csv(TOP_1M_DOMAINS_FILE)
        return data

    @task()
    def get_world_r_kulikov_28(data):
        world = data[data['Year'] == 1994 + hash(f'{login}') % 23] \
                    .groupby('Name', as_index = False) \
                    .agg({'Global_Sales': 'sum'}) \
                    .sort_values('Global_Sales', ascending = False) \
                    .iloc[0]
        return world.to_csv(index=False)
    
    @task()
    def get_europe_r_kulikov_28(data):
        europe = data[data['Year'] == 1994 + hash(f'{login}') % 23] \
                    .groupby('Genre', as_index = False) \
                    .agg({'EU_Sales': 'sum'}) \
                    .sort_values('EU_Sales', ascending = False) \
                    .iloc[0]
        return europe.to_csv(index=False)
    
    @task()
    def get_america_r_kulikov_28(data):
        america = data[data['Year'] == 1994 + hash(f'{login}') % 23] \
                    .groupby(['Name','Platform'], as_index = False) \
                    .agg({'NA_Sales': 'sum'}) \
                    .sort_values('NA_Sales', ascending = False) \
                    .query('NA_Sales > 1') \
                    .groupby('Platform', as_index = False) \
                    .agg({'Name': 'count'}) \
                    .sort_values('Name', ascending = False) \
                    .iloc[0]
        return america.to_csv(index=False)
    
    @task()
    def get_japan_r_kulikov_28(data):
        japan = data[data['Year'] == 1994 + hash(f'{login}') % 23] \
                    .groupby('Publisher', as_index = False) \
                    .agg({'JP_Sales': 'mean'}) \
                    .sort_values('JP_Sales', ascending = False) \
                    .iloc[0]
        return japan.to_csv(index=False)
    
    @task()
    def get_diff_r_kulikov_28(data):
        temp = data[data['Year'] == 1994 + hash(f'{login}') % 23] \
                    .groupby('Name', as_index = False) \
                    .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})
        temp['difference'] = temp.EU_Sales > temp.JP_Sales
        diff = temp.query('difference > 0').shape[0]
        return diff.to_csv(index=False)

    @task()
    def print_data(world, europe, america, japan, diff):

        print(world)
        print(europe)
        print(america)
        print(japan)
        print(diff)

        
    data = get_data_r_kulikov_28()
    world = get_world_r_kulikov_28(data)
    europe = get_europe_r_kulikov_28(data)
    america = get_america_r_kulikov_28(data)
    japan = get_japan_r_kulikov_28(data)
    diff = get_diff_r_kulikov_28(data)
    
    print_data(world, europe, america, japan, diff)

r_kulikov_28_lesson_3 = r_kulikov_28_lesson_3()
