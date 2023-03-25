import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task

path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'o-safronova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2022, 7, 6),
    'schedule_interval': '00 22 * * *'
}

#1994 + hash(f'o-safronova')%23  - 2006

@dag(default_args=default_args, catchup=False)

def o_safronova_lesson_3():
    
    @task()
    def get_data():
        df_year = pd.read_csv(path).query('Year == 2006.0')
        return df_year

    #Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def best_sale(df_year):
        top_sale = df_year.groupby(['Name'], as_index = False).agg({'Global_Sales': 'sum'}).sort_values('Global_Sales', ascending = False).head(1)
        return top_sale.to_csv(index=False)

    @task()
    def get_genre_EU(df_year):
        #Игры какого жанра были самыми продаваемыми в Европе?
        #Не дано условие самых продаваемых, поэтому выбрала всё, что больше 75процентиля
        EU_df = df_year.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'}).sort_values('EU_Sales', ascending = False)
        EU_df['Prs'] = EU_df.EU_Sales.quantile(0.75)
        top_EU = EU_df.query('EU_Sales > Prs')['Genre']
        return top_EU.to_csv(index=False)

    @task()
    def get_platform_mln(df_year):
        #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
        #Перечислить все, если их несколько
        max_pl = df_year.query('NA_Sales > 1.0').groupby(['Platform'], as_index = False).agg({'Name': 'count'}).sort_values('Name', ascending = False)
        max_pl['max_'] = max_pl.Name.max()
        max_pl = max_pl.query('Name == max_')['Platform']
        return max_pl.to_csv(index=False)

    @task()
    def get_publisher_JP(df_year):
        #У какого издателя самые высокие средние продажи в Японии?
        #Перечислить все, если их несколько
        top_JP = df_year.groupby(['Publisher'], as_index = False).agg({'JP_Sales': 'mean'}).sort_values('JP_Sales', ascending = False).head(1)['Publisher']
        return top_JP.to_csv(index=False)


    @task()
    def get_EU_than_JP(df_year):
        #Сколько игр продались лучше в Европе, чем в Японии
        more_EU_than_JP = df_year.query('JP_Sales >0 ').query('EU_Sales > JP_Sales').Name.nunique()
        return more_EU_than_JP

    @task()

    def print_data(top_sale, top_EU, max_pl, top_JP, more_EU_than_JP):

        date = ''

        print(f'Самая продаваемая игра во всем мире {date}')
        print(top_sale)

        print(f'Самый продаваемый жанр игры в Европе {date}')
        print(top_EU)

        print(f'Платформы СА с более млн продаж {date}')
        print(max_pl)

        print(f'Издатель с самыми высокими средними продажами Японии {date}')
        print(top_JP)

        print(f'Кол-во игр лучше проданных в Европе чем в Америке {date}')
        print(more_EU_than_JP)

    df_year = get_data()
    top_sale = best_sale(df_year)
    top_EU = get_genre_EU(df_year)
    max_pl = get_platform_mln(df_year)
    top_JP = get_publisher_JP(df_year)
    more_EU_than_JP = get_EU_than_JP(df_year)
    print_data(top_sale, top_EU, max_pl, top_JP, more_EU_than_JP)

o_safronova_lesson_3 = o_safronova_lesson_3()