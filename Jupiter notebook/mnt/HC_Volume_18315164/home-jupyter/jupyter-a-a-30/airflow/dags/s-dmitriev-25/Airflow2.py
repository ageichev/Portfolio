import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow.decorators import dag, task

path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 's-dmitriev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 17),
    'schedule_interval': '0 10 */4 * *'
}

# Определим год, за какой будем смотреть данные
year = 1994 + hash(f's-dmitriev-25') % 23


@dag(default_args=default_args, catchup=False)
def s_dmitriev_25_airflow_L3():
    @task()
    def get_data(path):
        df = pd.read_csv(path)
        return df

    @task()
    def most_sales(df):
        most_sales = df[df['Year'] == year]. \
            groupby('Name', as_index=False). \
            agg({'Global_Sales': 'sum'}). \
            sort_values(by='Global_Sales', ascending=False). \
            iloc[0][0]
        return most_sales

    @task()
    def EU_most_sales_genre(df):
        top_3 = df[df['Year'] == year]. \
            groupby('Genre', as_index=False). \
            agg({'EU_Sales': 'sum'}). \
            sort_values(by='EU_Sales', ascending=False). \
            head(3)
        EU_most_sales_genre = top_3.Genre.to_list()
        return EU_most_sales_genre

    @task()
    def NA_best_platform(df):
        NA_best_platform = df[(df['NA_Sales'] > 1) & (df['Year'] == year)]. \
            groupby('Platform', as_index=False). \
            agg({'Name': 'nunique'}). \
            sort_values(by='Name', ascending=False). \
            iloc[0][0]
        return NA_best_platform

    @task()
    def JP_best_publisher(df):
        JP_best_publisher = df[df['Year'] == year]. \
            groupby('Publisher', as_index=False). \
            agg({'JP_Sales': 'mean'}). \
            sort_values(by='JP_Sales', ascending=False). \
            iloc[0][0]
        return JP_best_publisher

    @task()
    def EU_more_than_JP_ngame(df):
        EU_more_than_JP_ngame = df[(df['JP_Sales'] < df['EU_Sales']) & (df['Year'] == year)]. \
            Name.nunique()
        return EU_more_than_JP_ngame

    @task()
    def print_data(most_sales,
                   EU_most_sales_genre,
                   NA_best_platform,
                   JP_best_publisher,
                   EU_more_than_JP_ngame):
        print(f'Bestseller game {year}: {most_sales}',
              f'Most popular genre in EU {year}: {EU_most_sales_genre}',
              f'Most popular platform in NA {year}: {NA_best_platform}',
              f'Bestseller publisher in JP {year}: {JP_best_publisher}',
              f'Number of games better sold in EU than in JP in {year}: {EU_more_than_JP_ngame}', sep='\n')

    df = get_data(path)

    most_sales_result = most_sales(df)
    EU_most_sales_genre_result = EU_most_sales_genre(df)
    NA_best_platform_result = NA_best_platform(df)
    JP_best_publisher_result = JP_best_publisher(df)
    EU_more_than_JP_ngame_result = EU_more_than_JP_ngame(df)

    print_data(most_sales_result,
               EU_most_sales_genre_result,
               NA_best_platform_result,
               JP_best_publisher_result,
               EU_more_than_JP_ngame_result)

s_dmitriev_25_airflow_L3 = s_dmitriev_25_airflow_L3()