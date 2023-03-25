import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context

default_args = {
    'owner': 'tizadornova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 5, 10),
    'schedule_interval': '00 12 * * *'
} 


@dag(default_args=default_args, catchup=False)
def tzadornova_dag_of_less3():
    
    @task(retries=3)
    def get_data():
        
        vg_sale = pd.read_csv('vgsales.csv')
        vg_sale['Year'] = vg_sale['Year'].fillna(0)
        vg_sale.Publisher = vg_sale.Publisher.replace(np.nan, 0)
        vg_sale = vg_sale.drop_duplicates()
        
        login = 'tizadornova'
        year_cur = 1994 + hash(f'{login}') % 23
        games_data = vg_sale[vg_sale['Year'] == year_cur]          
        return games_data
    
    
    
    @task(retries=3)
    def top_world_game(data):
        
        df_top_world_game = data[data.Global_Sales == data.Global_Sales.max()].iloc[0].Name

        return df_top_world_game
    
    @task(retries=3)
    def top_eu_genres(data):
        
        df_top_eu_genres = data.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'})
        
        df_top_eu_genres = df_top_eu_genres[df_top_eu_genres['EU_Sales'] == df_top_eu_genres['EU_Sales'].max()].iloc[0].Genre
        
        return df_top_eu_genres
    
    
    @task(retries=3)
    def top_na_platform(data):
        
        df_na_platform = data.query('NA_Sales > 1') \
                                .groupby('Platform', as_index=False) \
                                .agg({'Genre': 'count'}) \
                                .rename(columns={'Genre': 'Sum_sales'})
    
        df_top_na_platform = df_na_platform[df_na_platform['Sum_sales'] == df_na_platform.Sum_sales.max()].iloc[0].Platform

        return df_top_na_platform
    
    
    @task(retries=3)
    def top_jap_avg_sales(data):
        
        df_avg_jap_sales = data.groupby('Publisher', as_index=False) \
                                .agg({'JP_Sales': 'mean'}) \
                                .rename(columns={'JP_Sales': 'Avg_Sales'})

        df_top_jap_pub_sales = df_avg_jap_sales[df_avg_jap_sales['Avg_Sales'] == df_avg_jap_sales['Avg_Sales'].max()].iloc[0].Publisher

        return df_top_jap_pub_sales
    
    
    
    @task(retries=3)
    def eu_jap_sales(data):
        df_eu_jap_sales = data.groupby('Name', as_index=False).agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})

        df_eu_jap_sales['More_Sales'] = np.where(df_eu_jap_sales['EU_Sales'] > df_eu_jap_sales['JP_Sales'], 'Europe', 'Japan')
        count_eu_bigger_jap = df_eu_jap_sales.query('More_Sales == "Europe"').shape[0]

        return count_eu_bigger_jap
    
    
    @task(retries=3)
    def print_data(top_world_game, top_eu_genres, top_na_platform, top_jap_avg_sales, eu_jap_sales):
        
        
        login = 'tizadornova'
        year_cur = 1994 + hash(f'{login}') % 23
        
        
        print(f'''Data for                                                          {year_cur} 
                  Top video game around the world:                                  {top_world_game} 
                  Top genre in Europe:                                              {top_eu_genres}
                  Top platform in North America:                                    {top_na_platform}
                  Top publisher in Japan:                                           {top_jap_avg_sales}
                  The number of games that have more sales in Europe than in Japan: {eu_jap_sales}''')
        
        
    data = get_data()

    top_world_game = top_world_game(data)
    top_eu_genres = top_eu_genres(data)
    top_na_platform = top_na_platform(data)
    top_jap_avg_sales = top_jap_avg_sales(data)
    eu_jap_sales = eu_jap_sales(data)
    

    print_data(top_world_game, top_eu_genres, top_na_platform, top_jap_avg_sales, eu_jap_sales)

tzadornova_dag_of_less3 = tzadornova_dag_of_less3() 