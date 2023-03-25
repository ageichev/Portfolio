
import requests
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
    
file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year=1994+hash(f'{"a-abdibekova"}')%23 

default_args = {
    'owner': 'a.abdibekova',
    'depends_on_past': False,
    'retries': 2, 
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 7), 
    'schedule_interval': '55 21 * * *' 
} 
@dag(default_args=default_args, catchup=False) 
def abdibekova_3_airflow(): 
          
    @task() 
    def get_game_data(): 
        df =pd.read_csv(file).query('Year == @YEAR')
        return df 
    
    #самоя продаваемая игра в этом году во всем мире  
    @task() 
    def best_seller_game(df): 
        game=df.groupby('Name').agg({'Global_Sales':'sum'})
        top_game = game[game['Global_Sales']==game.Global_Sales.max()].Name.to_csv(index=False, header=False)
        return top_game 
 
    @task() 
    def genre_in_EU(df): 
        genre=df.groupby('Genre').agg({'EU_Sales':'sum'}).sort_values(by=['EU_Sales'],ascending=False)
        genre_in_EU = genre[genre['EU_Sales']==genre.EU_Sales.max()].Genre.to_csv(index=False, header=False)
        return genre_in_EU 
              
    @task() 
    def NA_platform(df): 
        NA_platform=df[df['NA_Sales']>1].groupby('Platform').agg({'NA_Sales':'count'}).sort_values(by=['NA_Sales'],ascending=False)  
        max_NA=NA_platform[NA_platform['NA_Sales']==NA_platform['NA_Sales'].max()].Platform.to_csv(index=False, header=False) 
        return max_NA 
    @task() 
    def JP_publisher(df): 
        JP=df.groupby('Publisher').agg({'JP_Sales':'mean'}).sort_values(by=['JP_Sales'],ascending=False)
        JP_publisher=JP[JP['JP_Sales']==JP['JP_Sales'].max()]['Publisher'].Publisher.to_csv(index=False, header=False)
        return JP_publisher 
    
    @task() 
    def compared_sales(df): 
        compared_sales=df.query('EU_Sales > JP_Sales').shape[0]
        return compared_sales
                                                                     
                  
    @task() 
    def print_data(top_game, genre_in_EU, max_NA, JP_publisher, compared_sales): 
        context = get_current_context() 
        date = context['ds'] 
      
        print(f'The best selling game in {year} in the world{top_Game}') 
    
              
        print(f'The best selling genre in {year} in EU {genre_in_EU}') 
     
              
        print(f'The best selling game platform in {year}  in NA {max_NA}') 
      
        print(f'The best selling game publisher in {year} in JP {JP_publisher}') 
               
        print(f'The number of games sold in {year} in EU is more than in JP{compared_sales}') 
        
    
    df=get_game_data()
    top_game=best_seller_game(df)                            .
    genre_in_EU=genre_in_EU(df) 
    max_NA=NA_platform(df) 
    JP_publisher=JP_publisher(df) 
    compared_sales=compared_sales(df) 
    print_data(top_game, genre_in_EU, max_NA, JP_publisher, compared_sales): 
             
abdibekova_3_airflow = abdibekova_3_airflow()  