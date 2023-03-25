import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


VGSALES = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
LOGIN = 'v-trifonov-20'
YEAR = 1994 + hash(f'{LOGIN}') % 23


default_args = {
    'owner': 'v-trifonov-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2022, 6, 28),
}
schedule_interval = '0 10 * * *'


@dag(default_args=default_args, catchup=False)
def v_trifonov_20_lesson_3_airflow():
    
    #Считывание данных
    @task()
    def get_data(VGSALES):
        vgsales = pd.read_csv(VGSALES)
        vgsales_year = vgsales[vgsales.Year == YEAR]
        
        return vgsales_year
    
    
    # Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def get_top_sales_world(vgsales_year):
        sales_world = vgsales_year.sort_values('Global_Sales', ascending=False)
        top_sales_world = sales_world.head(1).values[0][1]
        
        return top_sales_world
    
    
    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def get_top_sales_eu(vgsales_year):
        sales_eu = vgsales_year.groupby('Genre', as_index=False).agg({'EU_Sales': 'mean'}).sort_values('EU_Sales', ascending=False)
        top_sales_eu = sales_eu.max().values[:]
        
        return top_sales_eu
    
    
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько¶
    @task()
    def get_top_platform_na(vgsales_year):
        platform_na = vgsales_year.query('NA_Sales > 1').value_counts('Platform')
        top_platform_na = platform_na.idxmax()
        
        return top_platform_na
    
    
    # У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def get_top_sales_publisher_jp(vgsales_year):
        sales_publisher_jp = vgsales_year.groupby('Publisher', as_index=False) \
                                    .agg({'JP_Sales': 'mean'}) \
                                    .sort_values('JP_Sales', ascending=False)
        top_sales_publisher_jp = sales_publisher_jp[sales_publisher_jp.JP_Sales == sales_publisher_jp.JP_Sales.max()].values[:]
        
        return top_sales_publisher_jp

    
    # Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def get_number_game_sold_better_eu_from_jp(vgsales_year):
        number_game_sold_better_eu_from_jp = vgsales_year[vgsales_year['EU_Sales'] > vgsales_year['JP_Sales']].Rank.count()

        return number_game_sold_better_eu_from_jp
    
    
    # Пишем в лог
    @task()            
    def print_data(top_sales_world, top_sales_eu, top_platform_na, top_sales_publisher_jp, number_game_sold_better_eu_from_jp):
        
        context = get_current_context()
        date = context['ds']
        
        print(f'Top sales world for date {date}')
        print(top_sales_world)

        print(f'Top sales eu for date {date}')
        print(top_sales_eu)
        
        print(f'Top platform na for date {date}')
        print(top_platform_na)
        
        print(f'Top sales publisher jp for date {date}')
        print(top_sales_publisher_jp)
        
        print(f'Number game sold better eu from jp for date {date}')
        print(number_game_sold_better_eu_from_jp)

    vgsales_year = get_data(VGSALES)
    
    top_sales_world = get_top_sales_world(vgsales_year)
    top_sales_eu = get_top_sales_eu(vgsales_year)
    top_platform_na = get_top_platform_na(vgsales_year)
    top_sales_publisher_jp = get_top_sales_publisher_jp(vgsales_year)
    number_game_sold_better_eu_from_jp = get_number_game_sold_better_eu_from_jp(vgsales_year)
    
    print_data(top_sales_world, top_sales_eu, top_platform_na, top_sales_publisher_jp, number_game_sold_better_eu_from_jp)

    
v_trifonov_20_lesson_3_airflow = v_trifonov_20_lesson_3_airflow()