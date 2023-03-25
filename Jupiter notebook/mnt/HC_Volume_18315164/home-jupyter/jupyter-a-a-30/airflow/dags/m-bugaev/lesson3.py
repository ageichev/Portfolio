import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


PATH = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'm-bugaev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 22),
}
schedule_interval = '0 12 * * *'

login = 'm-bugaev'
YEAR = 1994 + hash(f"{login}") % 23

@dag(default_args=default_args,
     catchup=False, 
     schedule_interval=schedule_interval,
     tags=["Bugaev", "Michail", "lesson3"])

def m_bugaev_DAG_lesson3():

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
    def NA_popular_platform(df_year):
        df = df_year[df_year['NA_Sales'] > 1]
        df = df.groupby('Platform').count().reset_index()
        na_max_platform = ','.join(df[df['Rank'] == df['Rank'].max()]['Platform'].values)
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
        print(f'For{date}. {max_sale} самая продаваемая игра во всем мире в {YEAR} году')
        print(f'For{date}. Игры  жанра {eu_max_sale} были самыми продаваемыми в Европе в {YEAR} году')
        print(f'For{date}. На платформе {na_max_platform} было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке в {YEAR} году')
        print(f'For{date}. Самые высокие продажи в Японии в {YEAR} году {jp_max_publisher_sales}')
        print(f'For{date}. Количество игр, которые продались лучше в Европе, чем в Японии в {YEAR} году, составило {eu_jp_sales}')

    df_year = get_game_year()
    bug_max_sale = max_sale(df_year)
    bug_eu_max_sale = eu_max_sale(df_year)
    bug_NA_popular_platform = NA_popular_platform(df_year)
    jp_max_publisher_sale = jp_max_publisher_sales(df_year)
    eu_jp_sale = eu_jp_sales(df_year)
    print_game(bug_max_sale, bug_eu_max_sale, bug_NA_popular_platform, jp_max_publisher_sale, eu_jp_sale)

m_bugaev_lesson3 = m_bugaev_DAG_lesson3()