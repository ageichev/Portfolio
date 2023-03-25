#import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


PATH = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'dm-birjukov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2023, 2, 4),
    'schedule_interval': '0 */3 * * *'
}

# hash('dm-birjukov')             -2344595697887199580
# hash(f"{login}") % 23           6
# 1994 + hash(f'{login}') % 23    2000 
login = 'dm-birjukov'
YEAR = 1994 + hash(f'{login}') % 23


@dag(default_args=default_args, catchup=False)
def homework3_dmbirjukov():    # важно: дефисов в названии быть не должно
     
    @task(retries=3)
    def get_game_data():
        df = pd.read_csv(PATH)
        df_year = df[df.Year == YEAR]
        return df_year

    # Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def max_sales(df_year):
        max_sales = df_year.groupby('Name').sum().nlargest(1, 'Global_Sales').index[0]
        return max_sales

    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def eu_max_sales(df_year):
        eu_max_sales = df_year.groupby('Genre').sum().nlargest(1, 'EU_Sales').index[0]
        return eu_max_sales
    
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    # Перечислить все, если их несколько
    @task()
    def na_max_platform(df_year):
        na_df = df_year[df_year['NA_Sales']>1]
        na_df = na_df.groupby('Platform').count().reset_index()
        na_max_platform = ','.join(na_df[na_df['Rank']==na_df['Rank'].max()]['Platform'].values)
        return na_max_platform
 
    # У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def jp_publisher_max_sales(df_year):
        jp_df = df_year.groupby('Publisher').mean().reset_index()
        jp_publisher_max_sales = ','.join(jp_df[jp_df['JP_Sales']==jp_df['JP_Sales'].max()]['Publisher'].values)
        return jp_publisher_max_sales
    
    # Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def eu_jp_sales(df_year):
        eu_jp = df_year.groupby('Name').sum().reset_index()
        eu_jp_sales = eu_jp[eu_jp['EU_Sales']>eu_jp['JP_Sales']]['Name'].nunique()
        return eu_jp_sales
    
    @task()
    def print_gamedata(max_sales, eu_max_sales, na_max_platform, jp_publisher_max_sales, eu_jp_sales):

        context = get_current_context()
        date = context['ds']

        print(f'For {date}: {max_sales}; {eu_max_sales}; {na_max_platform}; {jp_publisher_max_sales}; {eu_jp_sales}')

        
    df_year = get_game_data()
    max_sales = max_sales(df_year)
    eu_max_sales = eu_max_sales(df_year)
    na_max_platform = na_max_platform(df_year)
    jp_publisher_max_sales = jp_publisher_max_sales(df_year)
    eu_jp_sales = eu_jp_sales(df_year)
    print_gamedata = print_gamedata(max_sales, eu_max_sales, na_max_platform, jp_publisher_max_sales, eu_jp_sales)

    
homework3_dmbirjukov = homework3_dmbirjukov()
