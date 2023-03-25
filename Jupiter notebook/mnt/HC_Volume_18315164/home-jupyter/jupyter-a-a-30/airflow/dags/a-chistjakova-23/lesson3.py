import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


PATH = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'a-chistjakova-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 11),
}
schedule_interval = '0 12 * * *'

login = 'a-chistjakova-23'
YEAR = 1994 + hash(f"{login}") % 23

@dag(default_args=default_args,
     catchup=False, 
     schedule_interval=schedule_interval,
     tags=["chistjakova", "anna", "lesson3", "a-chistjakova-23"])
def a_chistyakova_23_lesson3_DAG():

    @task()
    def get_game_year():
        df_year = pd.read_csv(PATH).query('Year == @YEAR')
        return df_year

    @task()
    def max_sale(df_year):
        max_sales = df_year.groupby('Name').sum().nlargest(1,'Global_Sales').index[0]
        return max_sales

    @task()
    def eu_max_sale(df_year):
        eu_max_sales = df_year.groupby('Genre').sum().nlargest(1,'EU_Sales').index[0]
        return eu_max_sales

    @task()
    def na_max_platform(df_year):
        na_df = df_year[df_year['NA_Sales'] > 1]
        na_df = na_df.groupby('Platform').count().reset_index()
        na_max_platform = ','.join(na_df[na_df['Rank'] == na_df['Rank'].max()]['Platform'].values)
        return na_max_platform

    @task()
    def jp_max_publisher_sales(df_year):
        jp_df = df_year.groupby('Publisher').mean().reset_index()
        jp_max_publisher_sales = ','.join(jp_df[jp_df['JP_Sales'] == jp_df['JP_Sales'].max()]['Publisher'].values)
        return jp_max_publisher_sales

    @task()
    def eu_jp_sales(df_year):
        eu_jp = df_year.groupby('Name').sum().reset_index()
        eu_jp_sales = eu_jp[eu_jp['EU_Sales'] > eu_jp['JP_Sales']]['Name'].nunique()
        return eu_jp_sales

    @task()
    def print_game(max_sale, eu_max_sale, na_max_platform, jp_max_publisher_sales, eu_jp_sales):
        context = get_current_context()
        date = context['ds']
        print(f'For{date}.Это самая продаваемая игра во всем мире в {YEAR} году')
        print(max_sale)
        print(f'For{date}.Игры этого жанра были самыми продаваемыми в Европе в {YEAR} году')
        print(eu_max_sale)
        print(f'For{date}.На этой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке в {YEAR} году')
        print(na_max_platform)
        print(f'For{date}.У этого издателя самые высокие средние продажи в Японии в {YEAR} году')
        print(jp_max_publisher_sales)
        print(f'For{date}.Количество игр, которые продались лучше в Европе, чем в Японии в {YEAR} году')
        print(eu_jp_sales)


    df_year = get_game_year()
    max_sale_a = max_sale(df_year)
    eu_max_sale_a = eu_max_sale(df_year)
    na_max_platform_a = na_max_platform(df_year)
    jp_max_publisher_sales_a = jp_max_publisher_sales(df_year)
    eu_jp_sales_a = eu_jp_sales(df_year)
    print_game(max_sale_a, eu_max_sale_a, na_max_platform_a, jp_max_publisher_sales_a, eu_jp_sales_a)

a_chistyakova_23_lesson3 = a_chistyakova_23_lesson3_DAG()
