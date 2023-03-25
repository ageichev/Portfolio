import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def most_selling_game():
    df = pd.read_csv('vgsales.csv')
    year = 1994 + hash(f'v-skripskaja-29') % 23

    df_2011 = df[df['Year'] == year]
    most_selling_game = df_2011[df_2011['Global_Sales'] == df_2011['Global_Sales'].max()]['Name'].item()
    with open('most_selling_game.csv', 'w') as f:
        f.write(most_selling_game.to_csv(index=False, header=False))



def genre_EU():
    df = pd.read_csv('vgsales.csv')
    year = 1994 + hash(f'v-skripskaja-29') % 23

    df_2011 = df[df['Year'] == year]
    genre_EU = df_2011[df_2011['EU_Sales'] == df_2011['EU_Sales'].max()]['Genre'].item()
    with open('genre_EU.csv', 'w') as f:
        f.write(genre_EU.to_csv(index=False, header=False))



def platform_NA():
    df = pd.read_csv('vgsales.csv')
    year = 1994 + hash(f'v-skripskaja-29') % 23

    df_2011 = df[df['Year'] == year]
    platform_NA = df_2011[df_2011['NA_Sales'] > 1]['Platform'].value_counts().index[0]
    with open('platform_NA.csv', 'w') as f:
        f.write(platform_NA.to_csv(index=False, header=False))

    
def publisher_JP():
    df = pd.read_csv('vgsales.csv')
    year = 1994 + hash(f'v-skripskaja-29') % 23

    df_2011 = df[df['Year'] == year]
    publisher_JP = df_2011.groupby(['Publisher']).agg({'JP_Sales':'mean'}).sort_values(by='JP_Sales', ascending=False).index[0]
    with open('publisher_JP.csv', 'w') as f:
        f.write(publisher_JP.to_csv(index=False, header=False))

        
def count_games_EU_JP():
    df = pd.read_csv('vgsales.csv')
    year = 1994 + hash(f'v-skripskaja-29') % 23

    df_2011 = df[df['Year'] == year]
    count_games_EU_JP = df_2011[df_2011['EU_Sales'] > df_2011['JP_Sales']].shape[0]
    with open('count_games_EU_JP.csv', 'w') as f:
        f.write(count_games_EU_JP.to_csv(index=False, header=False))


def print_data(ds): # передаем глобальную переменную airflow
    with open('most_selling_game.csv', 'r') as f:
        all_data_1 = f.read()
    with open('genre_EU.csv', 'r') as f:
        all_data_2 = f.read()
    with open('platform_NA.csv', 'r') as f:
        all_data_3 = f.read()
    with open('publisher_JP.csv', 'r') as f:
        all_data_4 = f.read()
    with open('count_games_EU_JP.csv', 'r') as f:
        all_data_5 = f.read()
    

    print(f''' 1) Most selling game {all_data_1}
        2) Most selling genre in Europe {all_data_2}
        3) Platform in NA with best sales {all_data_3}
        4) Publisher with best avg sales in JP {all_data_4}
        5) Count game with best sales in EU vs JP {all_data_5} ''')
    
default_args = {
    'owner': 'v-skripskaja-29',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 7),
    'schedule_interval': '0 10 * * *'
}

dag = DAG('v-skripskaja-29-games', default_args=default_args)

t1 = PythonOperator(task_id='most_selling_game',
                    python_callable=most_selling_game,
                    dag=dag)

t2 = PythonOperator(task_id='genre_EU',
                    python_callable=genre_EU,
                    dag=dag)

t3 = PythonOperator(task_id='platform_NA',
                        python_callable=platform_NA,
                        dag=dag)

t4 = PythonOperator(task_id='publisher_JP',
                        python_callable=publisher_JP,
                        dag=dag)

t5 = PythonOperator(task_id='count_games_EU_JP',
                        python_callable=count_games_EU_JP,
                        dag=dag)


t6 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


[t1, t2, t3, t4, t5] >> t6
