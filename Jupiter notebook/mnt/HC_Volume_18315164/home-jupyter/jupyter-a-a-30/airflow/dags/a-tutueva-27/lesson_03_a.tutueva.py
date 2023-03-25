import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow.decorators import dag, task

DATASET = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
YEAR = 1994 + hash(f'a-tutueva') % 23

default_args = {
    'owner': 'a-tutueva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 20),
    'schedule_interval': '30 14 * * *'
}

@dag(default_args=default_args, catchup=False)
def a_tutueva_vgsales():
    @task()
    def get_data():
        df = pd.read_csv(DATASET)
        df = df[df.Year == YEAR].reset_index()
        return df

    @task()
    def get_top_global(df):
        top_global = df.groupby('Name', as_index=False).agg({'Global_Sales':'sum'}) \
        .sort_values('Global_Sales', ascending=False).head(1)
        return top_global.to_string(index=False)

    @task()
    def get_top_eu(df):
        top_eu = df.groupby('Genre', as_index=False).agg({'EU_Sales':'sum'})
        max_eu_sales = top_eu.EU_Sales.max()
        top_eu = top_eu[top_eu.EU_Sales == max_eu_sales]
        return top_eu.to_string(index=False)

    @task()
    def get_top_na(df):
        top_na = df[df.NA_Sales > 1].groupby(['Platform','Name'], as_index=False).agg({'Name':'count'})
        top_na = top_na.groupby('Platform',as_index=False).agg({'Name':'sum'}).rename(columns={'Name':'Quantity'})
        max_na_quantity = top_na.Quantity.max()
        top_na = top_na[top_na.Quantity == max_na_quantity]
        return top_na.to_string(index=False)

    @task()
    def get_top_jp(df):
        top_jp = df.groupby('Publisher', as_index=False).agg({'JP_Sales':'mean'})
        max_jp_mean_sales = top_jp.JP_Sales.max()
        top_jp = top_jp[top_jp.JP_Sales == max_jp_mean_sales]
        return top_jp.to_string(index=False)

    @task()
    def get_eu_better_jp(df):
        sales_eu_better_jp = df.groupby('Name', as_index=False).agg({'EU_Sales':'sum', 'JP_Sales':'sum'})
        sales_eu_better_jp = sales_eu_better_jp[sales_eu_better_jp.EU_Sales > sales_eu_better_jp.JP_Sales].Name.nunique()
        return sales_eu_better_jp

    @task()
    def print_data(top_global, top_eu, top_na, top_jp, sales_eu_better_jp):
        print(
f'''Sales data for {YEAR} year.
Top Global sales game:
{top_global}

Top EU sales by genre:
{top_eu}

Top NA sales by platform:
{top_na}

Top JP sales by publisher:
{top_jp}

Quantity of games sold better in EU than JP:
{sales_eu_better_jp}
''')

    df = get_data()
    top_global = get_top_global(df)
    top_eu = get_top_eu(df)
    top_na = get_top_na(df)
    top_jp = get_top_jp(df)
    sales_eu_better_jp = get_eu_better_jp(df)
    print_data(top_global, top_eu, top_na, top_jp, sales_eu_better_jp)

a_tutueva_vgsales = a_tutueva_vgsales()
