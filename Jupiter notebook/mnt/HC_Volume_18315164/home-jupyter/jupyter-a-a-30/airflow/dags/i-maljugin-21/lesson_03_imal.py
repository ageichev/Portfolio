import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow.decorators import dag, task
import telegram
from airflow.operators.python import get_current_context
from airflow.models import Variable

default_args = {
    'owner': 'i-maljugin-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 3),
    'start_date': datetime(2022, 6, 27),
    'schedule_interval' : '11 21 */1 * *'
}

vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
CHAT_ID = 140657342
BOT_TOKEN = '5552488380:AAHp0LLyRxJnH9q-3PQ9WfVlwiAbkN8eJ4Y' # не разобрался с переменными окружения
#try:
#   BOT_TOKEN = Variable.get('telegram_secret')
#except:
#    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    #if BOT_TOKEN != '':
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)
    #else:
        #pass
    
    
    
    
year_for_survey = 1994 + hash(f'i-maljugin-21') % 23

@dag(default_args=default_args, catchup=False)
def i_mal_games():
    
    @task(retries=1)
    def get_data():
        # task 1. read data
        df = pd.read_csv(vgsales) # 
        df = df[df.Year == year_for_survey] #dropna() дубляжей нет, проверили, поэтому не пишем
        return  df
    
    
    @task(retries=1)
    def t_2(df):
        # task 2. Top saled game
        t_2 = df.sort_values('Global_Sales', ascending = False)
        #t_2 = t_2.iloc[0]['Name']
        #t_2 = t_2.iat[0,1]
        t_max = t_2.Global_Sales.max()
        t_2 = t_2[t_2.Global_Sales == t_max].Name.to_list()
        #t_2 = t_2[0]
        return t_2
    
    
    @task(retries=1)
    def t_3(df):
        # task 3. Top Genres in europe
        t_3 = df \
            .groupby('Genre', as_index = False) \
            .agg({'EU_Sales' : 'sum'})
        max_sales_eu = t_3.EU_Sales.max()
        t_3 = t_3[t_3.EU_Sales == max_sales_eu].Genre.to_list()
        return t_3
    
    
    @task(retries=1)
    def t_4(df):
        # taks 4. Platfrom with most games > 1kk copies in NA
        t_4 = df \
            .query('NA_Sales > 1') \
            .groupby('Platform', as_index = False) \
            .agg({'Name' : 'count'})
        max_sales_na = t_4.Name.max()
        t_4 = t_4[t_4.Name == max_sales_na].Platform.to_list()
        return t_4
    
    
    @task(retries=1)
    def t_5(df):
        # task 5. Most avg sales in JP
        t_5 = df \
            .groupby('Publisher', as_index = False) \
            .agg({'JP_Sales' : 'mean'})
        max_sales_publisher = t_5.JP_Sales.max()
        t_5 = t_5[t_5.JP_Sales == max_sales_publisher].Publisher.to_list()
        return t_5


    @task(retries=1)
    def t_6(df):
        #task 6. How many games have been soled better in EU then in JP
        EU_and_JP = df.query('EU_Sales > JP_Sales and JP_Sales > 0').shape[0]
        EU_no_JP = df.query('EU_Sales > JP_Sales and JP_Sales == 0').shape[0]
        if EU_and_JP > 0:
            eu_better_jp = EU_and_JP
        else:
            eu_better_jp = 'no'
        if EU_no_JP > 0:
            eu_better_no_jp = EU_no_JP
        else:
            eu_better_no_jp = 'no'
        eu_vs_jp ={'eu_better_jp' : eu_better_jp, 'eu_better_no_jp' : eu_better_no_jp}
        return eu_vs_jp
    
    @task(on_success_callback=send_message)
    def print_data(t_2, t_3, t_4, t_5, eu_vs_jp):
        eu_better_jp = eu_vs_jp['eu_better_jp']
        eu_better_no_jp = eu_vs_jp['eu_better_no_jp']
        print(f'Какая игра была самой продаваемой в этом году во всем мире? - {t_2}')
        print(f'Игры какого жанра были самыми продаваемыми в Европе? - {t_3}')
        print(f'На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?- {t_4}')
        print(f'У какого издателя самые высокие средние продажи в Японии?- {t_5}')
        print(f'Сколько игр продались лучше в Европе, чем в Японии? Если игра продавалась и в Европе и в Японии, то {eu_better_jp}')
        print(f'Сколько игр продались лучше в Европе, чем в Японии? Если игра продавалась в Европе, но не в Японии, то {eu_better_no_jp}')

        
    
    
    
    df = get_data()
    t_2 = t_2(df)
    t_3 = t_3(df)
    t_4 = t_4(df)
    t_5 = t_5(df)
    eu_vs_jp = t_6(df)
    
    print_data(t_2, t_3, t_4, t_5, eu_vs_jp)
    
i_mal_games = i_mal_games()