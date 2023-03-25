import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


default_args = {
    'owner': 'v-trifonov-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2022, 6, 26),
}
schedule_interval = '0 10 * * *'


@dag(default_args=default_args, catchup=False)
def v_trifonov_20_lesson_3_airflow_2():
    @task()
    def get_top_10_domen():
        top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
        top_data_domens = top_data_df['domain'].apply(lambda x: x.split('.')[1])
        top_data_top_10_domens = top_data_domens.value_counts()[:10]
        
        return top_data_top_10_domens

    
    @task()
    def get_domen_long_name():
        top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
        top_data_name_domain = top_data_df['domain'].apply(lambda x: x.split('.')[0])
        top_data_long_name_domain = top_data_name_domain[top_data_name_domain.transform(len) == top_data_name_domain.transform(len).max()].sort_values(ignore_index=True)[0]
        
        return top_data_long_name_domain

    
    @task()
    def get_domain_index():
        top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
        domain = "airflow.com"
        domain_index = top_data_df.query('domain == @domain')
        if domain_index.shape[0] != 0:
            domain_index_str = f'Domain index {domain} = {domain_index.values[0][0]}'
        else:
            domain_index_str = f'Domain "{domain}" is not found!'
            
        return domain_index_str

    @task()            
    def print_data(top_data_top_10_domens, top_data_long_name_domain, domain_index):
        
        context = get_current_context()
        date = context['ds']
        
        print(f'Top 10 domains for date {date}')
        print(top_data_top_10_domens)

        print(f'Top long domains for date {date}')
        print(top_data_long_name_domain)

        print(domain_index)
    
    
    top_data_top_10_domens = get_top_10_domen()
    top_data_long_name_domain = get_domen_long_name()
    domain_index = get_domain_index()
    
    print_data(top_data_top_10_domens, top_data_long_name_domain, domain_index)

v_trifonov_20_lesson_3_airflow_2 = v_trifonov_20_lesson_3_airflow_2()