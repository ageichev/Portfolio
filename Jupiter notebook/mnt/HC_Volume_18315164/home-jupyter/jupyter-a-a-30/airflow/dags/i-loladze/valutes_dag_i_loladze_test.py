import requests
import pandas as pd
import numpy as np
import datetime
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


default_args = {
    'owner': 'i.loladze',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(2022, 11, 6),
    'schedule_interval': '0 14 * * *'
}


@dag(default_args=default_args, catchup=False)
def valutes_dag_i_loladze_test():
    @task(retries=2)
    def get_cbr_api_data():
        try:
            cbr_api_valutes = pd.read_json('https://www.cbr-xml-daily.ru/daily_json.js') 
        except:
            return pd.DataFrame([])

        cbr_api_valutes_columns = cbr_api_valutes.columns
        if ('Date' not in cbr_api_valutes_columns or 'Valute' not in cbr_api_valutes_columns):
            return pd.DataFrame([])
        return cbr_api_valutes

    @task()
    def check_cbr_api_data(cbr_api_valutes):
        if cbr_api_valutes.empty is True:
            return cbr_api_valutes
        try:
            cbr_api_valutes_checked = pd.json_normalize(cbr_api_valutes['Valute'])
        except:
            return pd.DataFrame([])

        cbr_valutes_columns = cbr_api_valutes_checked.columns
        if ('CharCode' not in cbr_valutes_columns or 'Nominal' not in cbr_valutes_columns 
            or 'Name' not in cbr_valutes_columns or 'Value' not in cbr_valutes_columns 
            or 'Previous' not in cbr_valutes_columns):
            return pd.DataFrame([])

        try:
            cbr_api_valutes_checked['Value'] = cbr_api_valutes_checked['Value'].astype('float64')
            cbr_api_valutes_checked['Previous'] = cbr_api_valutes_checked['Previous'].astype('float64')
        except:
            return pd.DataFrame([])
        return cbr_api_valutes_checked

    @task()
    def get_cbr_daily_report(valutes):
        if valutes.empty is True:
            return valutes

        valutes['% change'] = (valutes['Value'] - valutes['Previous']) / valutes['Previous'] * 100
        valutes['% change'] = np.trunc(valutes['% change'] * 100000) / 100000
        valutes = valutes.sort_values(['% change'], ascending=False)
        return valutes[['CharCode', 'Name', 'Nominal', 'Value', 'Previous', '% change']]

    @task()
    def get_fixer_api_data():
        today_date = datetime.datetime.today().strftime('%Y-%m-%d')
        week_ago_date = (datetime.datetime.today() - datetime.timedelta(6)).strftime('%Y-%m-%d')
        url = f"https://api.apilayer.com/fixer/timeseries?start_date={week_ago_date}&end_date={today_date}&base=RUB"
        payload = {}
        headers= {"apikey": "1XIL7pfMciDNVQVhJfqnPsBSPxAVqWji"}

        try:
            response = requests.request("GET", url, headers=headers, data = payload)
            fixer_api_valutes = pd.read_json(response.text)
        except:
            return pd.DataFrame([])

        fixer_api_data_columns = fixer_api_valutes.columns
        if ('start_date' not in fixer_api_data_columns or 'end_date' not in fixer_api_data_columns
            or 'base'	not in fixer_api_data_columns or 'rates' not in fixer_api_data_columns):
            return pd.DataFrame([])
        return fixer_api_valutes

    @task()
    def check_fixer_api_data(fixer_api_valutes, last_week_days):
        if fixer_api_valutes.empty is True or fixer_api_valutes['base'].min() != 'RUB':
            return fixer_api_valutes, fixer_api_valutes

        today_date = last_week_days[0]
        yesterday_date = last_week_days[1]
        data_indexes = [index.strftime('%Y-%m-%d') for index in fixer_api_valutes.index.tolist()]

        #checking data for fixer-daily report
        if (today_date not in data_indexes or yesterday_date not in data_indexes):
            return pd.DataFrame([])
        #checking data for fixer-weekly report
        for date in last_week_days:
            if date not in data_indexes:
                return pd.DataFrame([])
        return fixer_api_valutes

    @task()
    def get_fixer_daily_report(fixer_api_valutes_daily_checked, last_week_days):
        if fixer_api_valutes_daily_checked.empty is True:
            return fixer_api_valutes_daily_checked

        today_date = last_week_days[0]
        yesterday_date = last_week_days[1]
        try:
            today_valutes = pd.json_normalize(fixer_api_valutes_daily_checked[fixer_api_valutes_daily_checked.index == today_date]['rates'][0])
            yesterday_valutes = pd.json_normalize(fixer_api_valutes_daily_checked[fixer_api_valutes_daily_checked.index == yesterday_date]['rates'][0])
        except:
            return pd.DataFrame([])

        today_valutes = pd.melt(today_valutes, value_vars=today_valutes.columns, var_name='country', value_name='valute')
        yesterday_valutes = pd.melt(yesterday_valutes, value_vars=yesterday_valutes.columns, var_name='country', value_name='yesterday_valute')
        daily_report = today_valutes.merge(yesterday_valutes, how='inner', on='country')

        try:
            daily_report['valute'] = daily_report['valute'].astype('float64')
            daily_report['yesterday_valute'] = daily_report['yesterday_valute'].astype('float64')
        except:
            return pd.DataFrame([])

        daily_report['% change'] = (daily_report['valute'] - daily_report['yesterday_valute']) / daily_report['yesterday_valute'] * 100
        daily_report['% change'] = np.trunc(daily_report['% change'] * 100000) / 100000
        return daily_report.sort_values('% change', ascending=False)

    @task()
    def get_fixer_weekly_report(fixer_api_valutes_checked):
        if fixer_api_valutes_checked.empty is True:
            return fixer_api_valutes_checked

        all_fixer_api_data = pd.json_normalize(fixer_api_valutes_checked['rates'])
        all_fixer_api_data = pd.melt(all_fixer_api_data, value_vars=all_fixer_api_data.columns, var_name='country', value_name='valute')
        try:
            all_fixer_api_data['valute'] = all_fixer_api_data['valute'].astype('float64')
        except:
            return pd.DataFrame([])

        fixer_weekly_report = all_fixer_api_data \
                                .groupby('country', as_index=False) \
                                .agg({'valute': [np.mean, np.median, np.max, np.min, np.std]})
        return fixer_weekly_report

    @task()
    def send_dag_report_from_bot(cbr_daily_report, fixer_daily_report, fixer_weekly_report):
        bot_api = '5421552829%3AAAFPWH4hZBrw_RzU95rAZn1m60QHuUcnecU'
        url_send_message = f'https://api.telegram.org/bot{bot_api}/sendMessage'
        url_send_document = f'https://api.telegram.org/bot{bot_api}/sendDocument'
        chat_id = '403829853'
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')

        for report, report_name in ((cbr_daily_report, 'CBR Daily'), (fixer_daily_report, 'FIXER Daily'),
                                (fixer_weekly_report, 'FIXER Weekly')):
            if report.empty is True:
                response = requests.post(url_send_message, data={'chat_id': chat_id, 'text': f'{report_name} report was crashed during executing'})
                print(f'{report_name} report was crashed during executing')
            else:
                doc_sending_response = requests.post(url_send_document, data={'chat_id': chat_id,
                                                                              'file_name': 'sdds',
                                                                              'caption': f'{report_name} report for {current_date}'}, 
                                                     files={'document': report.to_csv(index=False)})
                print(doc_sending_response.text)
                print(report.head())

    last_week_days = []
    for i in range(7):
        last_week_days.append((datetime.datetime.now() - datetime.timedelta(i)).strftime('%Y-%m-%d'))
                
    cbr_api_valutes = get_cbr_api_data()
    cbr_api_valutes_checked = check_cbr_api_data(cbr_api_valutes)
    cbr_daily_report = get_cbr_daily_report(cbr_api_valutes_checked)

    fixer_api_valutes = get_fixer_api_data()
    fixer_api_valutes_checked = check_fixer_api_data(fixer_api_valutes, last_week_days) 
    fixer_daily_report = get_fixer_daily_report(fixer_api_valutes_checked, last_week_days)
    fixer_weekly_report = get_fixer_weekly_report(fixer_api_valutes_checked)

    send_dag_report_from_bot(cbr_daily_report, fixer_daily_report, fixer_weekly_report)

valutes_dag_i_loladze_test = valutes_dag_i_loladze_test()