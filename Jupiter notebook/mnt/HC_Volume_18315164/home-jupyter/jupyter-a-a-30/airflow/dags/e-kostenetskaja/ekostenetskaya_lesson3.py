import pandas as pd

from datetime import timedelta
from datetime import datetime


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#Заведем в переменную адрес, где лежит файл с данными
data_url = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

#Определим год, за какой будем смотреть данные
login = 'e-kostenetskaja'
year = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'e-kostenetskaja',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 1)
}

@dag(default_args=default_args, schedule_interval='0 12 * * *', catchup=False)
def games_sales_kostenetskaja():

    @task()
    def get_data():
        sales = pd.read_csv(data_url)
        #Отфильтруем исходные данные по полученному выше году
        sales = sales.query("Year == @year")
        return sales
    
#Какая игра была самой продаваемой в этом году во всем мире?     
    @task()
    def get_best_game(sales):
        best_game = sales.groupby('Name', as_index = False) \
        .agg({'Global_Sales' : 'sum'}) \
        .sort_values('Global_Sales', ascending = False) \
        .reset_index()
        best_game = best_game.loc[best_game['Global_Sales'] == best_game['Global_Sales'].max()]['Name']
        return best_game.to_csv(index=False, header = False)

#Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def get_bestsellers(sales):
        bestsellers = sales.groupby('Genre', as_index = False) \
        .agg({'EU_Sales' : 'sum'}) \
        .sort_values('EU_Sales', ascending = False) \
        .reset_index()
        bestsellers = bestsellers.loc[bestsellers['EU_Sales'] == bestsellers['EU_Sales'].max()]['Genre']
        return bestsellers.to_csv(index=False, header = False)
    
#На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько
    @task()
    def get_na_top_sales(sales):
        na_top_sales = sales.query("NA_Sales > 1") \
        .groupby('Platform', as_index = False) \
        .agg({'NA_Sales' : 'count'}) \
        .sort_values('NA_Sales', ascending = False) \
        .reset_index()
        na_top_sales = na_top_sales.loc[na_top_sales['NA_Sales'] == na_top_sales['NA_Sales'].max()]['Platform']
        return na_top_sales.to_csv(index=False, header = False)
    
#У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def get_ja_top_avg_sales(sales):
        ja_top_avg_sales = sales.groupby('Publisher', as_index = False) \
        .agg({'JP_Sales' : 'mean'}) \
        .sort_values('JP_Sales', ascending = False) \
        .reset_index()
        ja_top_avg_sales = ja_top_avg_sales.loc[ja_top_avg_sales['JP_Sales'] == ja_top_avg_sales['JP_Sales'].max()]['Publisher']
        return ja_top_avg_sales.to_csv(index=False, header = False)
    
#Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def get_sales_eu_better_ja(sales):
        sales_eu_better_ja = sales.groupby('Name', as_index = False) \
        .agg({'EU_Sales' : 'sum', 'JP_Sales' : 'sum'}) \
        .reset_index()
        sales_eu_better_ja = sales_eu_better_ja.loc[sales_eu_better_ja['EU_Sales'] > sales_eu_better_ja['JP_Sales']].shape[0]
        return sales_eu_better_ja
    
#Финальный таск который собирает все ответы
    @task()
    def print_data(best_game, bestsellers, na_top_sales, ja_top_avg_sales, sales_eu_better_ja):

        context = get_current_context()
        date = context['ds']

        print(f'''Дата формирования отчета: {date}
                  Год, за который были сформированы данные: {year}
                  Самая продаваемая игра в году во всем мире: {best_game}
                  Самые продаваемые жанры в Европе: {bestsellers}
                  Платформы, на которых было больше всего игр, проданных более чем миллионным тиражом в Северной Америке: {na_top_sales}
                  Издатели с самыми высокими средними продажами в Японии: {ja_top_avg_sales}
                  Какое количество игр было продано лучше в Европе, чем в Японии: {sales_eu_better_ja}''')

    sales = get_data()
    best_game = get_best_game(sales)
    bestsellers = get_bestsellers(sales)

    na_top_sales = get_na_top_sales(sales)
    ja_top_avg_sales = get_ja_top_avg_sales(sales)
    sales_eu_better_ja = get_sales_eu_better_ja(sales)

    print_data(best_game, bestsellers, na_top_sales, ja_top_avg_sales, sales_eu_better_ja)

games_sales_kostenetskaja = games_sales_kostenetskaja()