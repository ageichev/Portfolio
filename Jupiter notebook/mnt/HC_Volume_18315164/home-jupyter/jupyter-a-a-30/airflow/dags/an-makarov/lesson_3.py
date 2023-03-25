import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow.decorators import dag, task

data = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
start_year = 1994 + hash(f'an-makarov') % 23

default_args = {
    'owner': 'a.makarov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 23),
    'schedule_interval': '0 12 * * *'
}

@dag(default_args=default_args, catchup=False)
def video_games_sales_mkv():
    @task()
    def get_data():
        df = pd.read_csv(data)
        df = df[df.Year == start_year].reset_index()                              
        return df
    
    @task()
    def get_global_sale(df):
        top_sales_global = df.groupby(['Name','Year'], as_index=False)['Global_Sales'].sum().sort_values('Global_Sales',ascending=False).head(1)
        return top_sales_global.to_csv(index=False)    
    
    @task()
    def get_top_genre_eu_sale(df):
        top_genre_eu = df.groupby(['Genre'], as_index=False)['EU_Sales'].max().sort_values('EU_Sales', ascending=False).head(1)       
        return top_genre_eu.to_csv(index=False)    
    
    @task()
    def get_top_platform_NA(df):
        top_platforms_na = df[df.NA_Sales >= 1].groupby('Platform', as_index=False)['NA_Sales'].count() \
            .rename(columns = {'NA_Sales':'amount'}) \
            .sort_values('amount', ascending=False).head(3)
        return top_platforms_na.to_csv(index=False)    
    
    @task()
    def get_top_sale_mean_jp(df):
        top_sale_mean_jp = df.groupby('Publisher', as_index=False)['JP_Sales'].mean() \
            .rename(columns = {'JP_Sales':'mean_sales_jp'}) \
            .sort_values('mean_sales_jp', ascending = False).round(2).head(5)
        return top_sale_mean_jp.to_csv(index=False)
    
    @task()
    def get_eu_sales_higher_than_jp(df):
        eu_jp_sales = df.groupby('Name', as_index=False).agg({'EU_Sales':'sum', 'JP_Sales':'sum'})
        eu_jp_sales = eu_jp_sales[eu_jp_sales.EU_Sales > eu_jp_sales.JP_Sales].Name.nunique()
        return eu_jp_sales
    
    @task()
    def print_data(top_sales_global, top_genre_eu, top_platforms_na, top_sale_mean_jp, eu_sales_higher_than_jp):
            print(
                
f'''
Video games sales from {start_year}
Top global sales: 
{top_sales_global}
Best genres for EU:
{top_genre_eu}
Top sales by platform NA:
{top_platforms_na}
Top Japanese sales by mean:
{top_sale_mean_jp}
EU games that has better sales than JP:
{eu_jp_sales}

''')

    df = get_data()
    top_global = get_global_sale(df)
    top_eu = get_top_genre_eu_sale(df)
    top_na = get_top_platform_NA(df)
    top_jp = get_top_sale_mean_jp(df)
    eu_jp_sales = get_eu_sales_higher_than_jp(df)
    
    print_data(top_global, top_eu, top_na, top_jp, eu_jp_sales) 

video_games_sales_mkv = video_games_sales_mkv()