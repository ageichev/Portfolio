import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task


default_args = {
    'owner': 'o-kolegova-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 31),
    'schedule_interval': '0 9 * * *'
}

@dag(default_args=default_args, catchup=False)
def o_kolegova_22_game_report():
    @task()
    def get_data():
        sales = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        year = 1994 + hash('o-kolegova-22') % 23
        sales = sales.query('Year == @year')
        return sales

    @task()
    def get_best_selling_game(sales):
        best_selling_game = sales.groupby(
            'Name', as_index=False
        ).agg({'Global_Sales':'sum'}).sort_values(by='Global_Sales', ascending=False).head(1).iloc[0, 0]
        return best_selling_game

    @task()
    def get_best_selling_genre_europe(sales):
        best_genre = sales.groupby(
            'Genre', as_index=False
        ).agg({'EU_Sales':'sum'})
        max_sales = best_genre['EU_Sales'].max()
        best_selling_genre = best_genre.query('EU_Sales == @max_sales')
        return best_selling_genre

    @task()
    def get_best_plaform_north_america(sales):
        best_platform_north_america = sales.query('NA_Sales >= 1').groupby(
            'Platform', as_index=False
        ).agg({'NA_Sales':'sum'})
        max_sales = best_platform_north_america['NA_Sales'].max()
        best_platform_north_america = best_platform_north_america.query('NA_Sales == @max_sales')
        return best_platform_north_america

    @task()
    def get_best_avg_sale_publisher_japan(sales):
        best_avg_sale_publisher_japan = sales.groupby('Publisher', as_index=False).agg({'JP_Sales':'mean'})
        max_sales = best_avg_sale_publisher_japan['JP_Sales'].max()
        best_avg_sale_publisher_japan = best_avg_sale_publisher_japan.query('JP_Sales == @max_sales')
        return best_avg_sale_publisher_japan

    @task()
    def get_eu_more_than_jp(sales):
        sales_eu_jp = sales.groupby('Name', as_index=False).agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})
        eu_more_than_jp = sales_eu_jp.query('EU_Sales > JP_Sales').Name.count()
        return eu_more_than_jp

    @task()
    def print_data(best_selling_game, best_selling_genre, best_platform_north_america, best_avg_sale_publisher_japan, eu_more_than_jp):
        year = 1994 + hash('o-kolegova-22') % 23
        print(f'For the year {year}')
        print('Best selling game: {}'.format(best_selling_game))
        print('Best selling genre: {}'.format(', '.join(best_selling_genre.Genre.to_list())))
        print('Best selling platform in north america: {}'.format(', '.join(best_platform_north_america.Platform.to_list())))
        print('Best publisher in japan by average sales: {}'.format(', '.join(best_avg_sale_publisher_japan.Publisher.to_list())))
        print('Number of games which sell better in Europe than in Japan: {}'.format(eu_more_than_jp))
    
    sales = get_data()
    best_selling_game = get_best_selling_game(sales)
    best_selling_genre = get_best_selling_genre_europe(sales)
    best_platform_north_america = get_best_plaform_north_america(sales)
    best_avg_sale_publisher_japan = get_best_avg_sale_publisher_japan(sales)
    eu_more_than_jp = get_eu_more_than_jp(sales)

    print_data(best_selling_game, best_selling_genre, best_platform_north_america, best_avg_sale_publisher_japan, eu_more_than_jp)
    

o_kolegova_22_game_report = o_kolegova_22_game_report()
