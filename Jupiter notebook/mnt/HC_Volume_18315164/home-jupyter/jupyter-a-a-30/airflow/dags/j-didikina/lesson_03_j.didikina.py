import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow.decorators import dag, task

SALES_OF_GAMES = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
YEAR = 1994 + hash(f'j-didikina') % 23

default_args = {
    'owner': 'j_didikina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 5)
}

@dag(default_args=default_args, schedule_interval='30 13 * * *', catchup=False)
def didikina_vgsales_airflow_2():
    @task(retries=3)
    def get_data():
        sales = pd.read_csv(SALES_OF_GAMES).query('Year == @YEAR')
        return sales

    @task()
    def get_top_global(sales):
        top_global = sales.groupby('Name').agg({'Global_Sales':'sum'}).sort_values('Global_Sales', ascending=False).reset_index().head(1)
        return top_global

    @task()
    def get_top_eu(sales):
        top_eu = sales.groupby('Genre').agg({'EU_Sales':'sum'}).sort_values('EU_Sales', ascending=False).reset_index()
        max_eu = top_eu.EU_Sales.max()
        top_eu = top_eu.query('EU_Sales == @max_eu')
        return top_eu

    @task()
    def get_top_na(sales):
        top_na = sales.query('NA_Sales > 1').groupby(['Platform']).agg({'Name':'nunique'}).rename(columns={'Name':'Quantity'}).reset_index()
        max_na = top_na.Quantity.max()
        top_na = top_na.query('Quantity == @max_na')
        return top_na

    @task()
    def get_top_jp(sales):
        top_jp = sales.groupby(['Publisher']).agg({'JP_Sales':'mean'}).reset_index()
        max_jp = top_jp.JP_Sales.max()
        top_jp = top_jp.query('JP_Sales == @max_jp')
        return top_jp
    
    @task()
    def get_better(sales):
        better_games = sales.groupby('Name').agg({'EU_Sales':'sum','JP_Sales':'sum'}).reset_index().query('EU_Sales > JP_Sales').shape[0]
        return better_games

    @task()
    def print_data(top_global, top_eu, top_na, top_jp, better_games):
        top_global = top_global['Name'].to_string(index=False)
        top_eu = top_eu['Genre'].to_string(index=False)
        top_na = top_na['Platform'].to_string(index=False)
        top_jp = top_jp['Publisher'].to_string(index=False)
                
        print(f'''Sales data for {YEAR} year.
                  Top game by Global sales: {top_global}
                  Top genre by EU Sales: {top_eu}
                  Top platform by NA Sales: {top_na}
                  Top publisher by JP Sales: {top_jp}
                  Quantity of games sold better in EU than JP: {better_games}''')

    sales = get_data()
    top_global = get_top_global(sales)
    top_eu = get_top_eu(sales)
    top_na = get_top_na(sales)
    top_jp = get_top_jp(sales)
    better_games = get_better(sales)
    print_data(top_global, top_eu, top_na, top_jp, better_games)

didikina_vgsales_airflow_2 = didikina_vgsales_airflow_2()
