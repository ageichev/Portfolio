import pandas as pd
import numpy as np

from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from io import StringIO


path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
# path = '../airflow/dags/a.batalov/vgsales.csv'
# path = '../a.batalov/vgsales.csv'

login = 'i-kopytin-30'
year = 1994 + hash(f'{login}') % 23
# year


# NA_Sales - Sales in North America (in millions)
# EU_Sales - Sales in Europe (in millions)
# JP_Sales - Sales in Japan (in millions)
# Other_Sales - Sales in the rest of the world (in millions)
# Global_Sales - Total worldwide sales.

default_args = {
    'owner': 'i-kopytin-30',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 3),
    'start_date': datetime(2023, 2, 15)
}
schedule_interval =  '08 08 * * *'

@dag(default_args = default_args, schedule_interval = schedule_interval, catchup = False)
def i_kopytin_30_Les3():
    
    
    @task()
    def t0_get_data():
        # print(year)
        df = pd.read_csv(path)
        return df[df.Year == year].to_csv(index = False)


    # Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def t1_0_top_globsales(df_) -> str:
        '''
        Находит самую продаваемую игру,
        вышедшую в заданном году во всем мире.

        Возвращает название игры (строку).

        Учитывает мультиплатформенность.
        '''
        df = pd.read_csv(StringIO(df_),
                         header = 0)

        top_ = df.groupby('Name').Global_Sales.sum()
        return '\n'.join(top_[top_ == top_.max()].index.values)


    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def t1_1_top_eu_genre(df_) -> str:
        '''
        Находит жанр/ы, к которому/ым относятся самые
        продаваемые игры в Европе, вышедшие в заданном году.

        Возвращает строку, содержащую (через LF)
        названия игр.

        Учитывает мультиплатформенность.
        '''
        df = pd.read_csv(StringIO(df_),
                         header = 0)

        genres = df.groupby('Name').agg({'EU_Sales' : sum, 'Genre' : lambda x: x.iloc[0]}).groupby('Genre').EU_Sales.sum()
        return '\n'.join(genres[genres == genres.max()].index.values)

    # %%timeit
    # df.groupby('Name').agg({'EU_Sales' : sum, 'Genre' : min}).groupby('Genre').EU_Sales.sum()

    # %%timeit
    # df.groupby('Name').agg({'EU_Sales' : sum, 'Genre' : lambda x: x.iloc[0]}).groupby('Genre').EU_Sales.sum()


    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    # Перечислить все, если их несколько
    @task()
    def t1_2_top_na_platform(df_) -> str:
        '''
        Находит платформу/ы, на которой/ых
        было больше всего игр, проданных тиражом > 1M в Северной Америке.

        Возвращает строку, содержащую (через LF)
        названия платформ.

        Учитывает мультиплатформенность.
        '''
        df = pd.read_csv(StringIO(df_),
                         header = 0)

        platforms = df[df.NA_Sales > 1.0].Platform.value_counts()
        return '\n'.join(platforms[platforms == platforms.max()].index.values)


    # У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def t1_3_top_mean_jap_sales(df_) -> str:
        '''
        Находит издателя/ей, у игр которого/ых
        наибольшие средние продажи в Японии.

        Возвращает строку, содержащую (через LF)
        названия издателей.

        Учитывает мультиплатформенность.

            (В смысле не Nintendo??)
                и даже без ограничения по году выхода не Nintendo
        '''
        df = pd.read_csv(StringIO(df_),
                         header = 0)


        publishers = df.groupby('Name').agg({'JP_Sales' : sum, 'Publisher' : lambda x: x.iloc[0]}).\
                        groupby('Publisher').JP_Sales.mean()
        return '\n'.join(publishers[publishers == publishers.max()].index.values)


    # Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def t1_4_count_eu_target(df_) -> str:
        '''
        Считает, сколько игр продались в Европе лучше (больше продаж),
        чем в Японии.

        Возвращает строку, в которой через пробел идут искомое число
        и процент от числа игр за заданный год.

        Учитывает мультиплатформенность.
        '''
        df = pd.read_csv(StringIO(df_),
                         header = 0)

        games = df.groupby('Name').agg({'EU_Sales' : sum, 'JP_Sales' : sum})
        t1_4_res = games[games.EU_Sales > games.JP_Sales].shape[0]
        return f'''{t1_4_res}, {round(t1_4_res / games.shape[0], 2)}%'''


    # logging
    @task()
    def print_res(dict_):
        context = get_current_context()
        date = context['ds']
        for task_n in range(1, 6):
            print(f'''Task {task_n} result for {date}:\n{dict_[f"task_{task_n}"]},''')
        
    data = t0_get_data() # 0
    
    res_dict = {'task_1' : t1_0_top_globsales(data),
                'task_2' : t1_1_top_eu_genre(data),
                'task_3' : t1_2_top_na_platform(data),
                'task_4' : t1_3_top_mean_jap_sales(data),
                'task_5' : t1_4_count_eu_target(data)
               }
    
    print_res(res_dict)

_ = i_kopytin_30_Les3()
