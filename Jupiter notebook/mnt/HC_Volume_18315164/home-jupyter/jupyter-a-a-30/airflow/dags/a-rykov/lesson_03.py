import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task

vg_sales_file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 2000+hash(f'{"a-rykov"}')%23
                   
default_args = {
    'owner': 'a-rykov',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 20),
}

@dag(default_args=default_args, schedule_interval = '0 10 * * *', catchup=False)
def a_rykov_vg_sales_stat():
    @task()
    def get_df():
        vg_df = pd.read_csv(vg_sales_file)
        vg_sales = vg_df.query('Year == @year')
        return vg_sales

    @task()
    def global_sales(vg_sales):
        global_sales = vg_sales.query('Global_Sales == Global_Sales.max()')[['Name']]
        return global_sales.to_csv(index=False, header=False)

    @task()
    def eu_top_5_sales_by_genre(vg_sales):
        eu_top_5_sales_by_genre = vg_sales \
        .groupby('Genre', as_index = False) \
        .agg({'EU_Sales': 'sum'}) \
        .sort_values('EU_Sales', ascending = False) \
        .head()
        return eu_top_5_sales_by_genre.to_csv(index=False, header=False)

    @task()
    def na_top_sales_by_platform(vg_sales):
        na_top_sales_by_platform = vg_sales \
        .query('NA_Sales >= 1') \
        .groupby('Platform', as_index = False) \
        .agg({'Name': 'count'}) \
        .sort_values('Name', ascending = False) \
        .head(1)
        return na_top_sales_by_platform.to_csv(index=False, header=False)

    @task()
    def jp_top_mean_sales_by_publisher(vg_sales):
        jp_top_mean_sales_by_publisher = vg_sales \
        .groupby('Publisher', as_index = False) \
        .agg({'JP_Sales': 'mean'}) \
        .sort_values('JP_Sales', ascending = False) \
        .head(3)
        return jp_top_mean_sales_by_publisher.to_csv(index=False, header=False)

    @task()
    def vg_sales_eu_vs_jp(vg_sales):
        vg_sales_eu_vs_jp = len(vg_sales \
        .assign(EU_vs_JP = vg_sales.EU_Sales > vg_sales.JP_Sales) \
        .query('EU_vs_JP == 1'))    
        return vg_sales_eu_vs_jp

    @task()
    def print_data(global_sales, eu_top_5_sales_by_genre, na_top_sales_by_platform, jp_top_mean_sales_by_publisher, vg_sales_eu_vs_jp):
        print(f'''Global best-selling game of {year}: {global_sales}
                  EU top 5 sales by genre of {year}: {eu_top_5_sales_by_genre}
                  NA top sales by platform of {year}: {na_top_sales_by_platform}
                  JP top mean sales by publisher of {year}: {jp_top_mean_sales_by_publisher}
                  Value of games better sold in NA then JP of {year}: {vg_sales_eu_vs_jp}''')

    vg_sales = get_df()

    global_sales = global_sales(vg_sales)
    eu_top_5_sales_by_genre = eu_top_5_sales_by_genre(vg_sales)
    na_top_sales_by_platform = na_top_sales_by_platform(vg_sales)
    jp_top_mean_sales_by_publisher = jp_top_mean_sales_by_publisher(vg_sales)
    vg_sales_eu_vs_jp = vg_sales_eu_vs_jp(vg_sales)

    print_data(global_sales, eu_top_5_sales_by_genre, na_top_sales_by_platform, jp_top_mean_sales_by_publisher, vg_sales_eu_vs_jp)

a_rykov_vg_sales_stat = a_rykov_vg_sales_stat()