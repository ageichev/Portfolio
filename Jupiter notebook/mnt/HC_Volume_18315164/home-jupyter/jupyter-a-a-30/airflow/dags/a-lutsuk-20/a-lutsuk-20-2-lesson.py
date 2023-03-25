{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {},
   "outputs": [],
   "source": [
    "import requests\n",
    "import pandas as pd\n",
    "from datetime import timedelta\n",
    "from datetime import datetime\n",
    "\n",
    "from airflow import DAG\n",
    "from airflow.operators.python import PythonOperator"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "metadata": {},
   "outputs": [],
   "source": [
    "TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'\n",
    "TOP_1M_DOMAINS_FILE = 'top-1m.csv'"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "metadata": {},
   "outputs": [],
   "source": [
    "def aalutsuk_get_data():\n",
    "    top_doms = pd.read_csv(TOP_1M_DOMAINS)\n",
    "    top_data = top_doms.to_csv(index=False)\n",
    "\n",
    "    with open(TOP_1M_DOMAINS_FILE, 'w') as f:\n",
    "        f.write(top_data)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {},
   "outputs": [],
   "source": [
    "def aalutsuk_top_10_domain(): \n",
    "    \n",
    "    top_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])\n",
    "    \n",
    "    top_10_domain = (top_df.domain.apply(lambda x: x.split('.')[-1]).value_counts().to_frame().reset_index().head(10))\n",
    "    \n",
    "    top_10_domain.rename(columns= {'index' :'domain'}, inplace=True)\n",
    "    \n",
    "    with open ('top_10_domain.csv', 'w') as f:\n",
    "        f.write(top_10_domain.to_csv(index=False, header=False))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "metadata": {},
   "outputs": [],
   "source": [
    "def aalutsuk_max_length(): \n",
    "    \n",
    "    top_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])\n",
    "    \n",
    "    top_df['d_len'] = top_df['domain'].apply(lambda x: len(x))\n",
    "    \n",
    "    max_len = (top_df.sort_values(['d_len', 'domain'], ascending=[False, True]).reset_index().domain[0])\n",
    "    with open('max_len.txt', 'w') as f:\n",
    "        f.write(str(max_len))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "metadata": {},
   "outputs": [],
   "source": [
    "def aalutsuk_airflow(): \n",
    "    top_doms_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])\n",
    "    try:\n",
    "        air_rank = int(top_doms_df.query(\"domain == 'airflower.ru'\")['rank'])\n",
    "    except:\n",
    "        any_air = (top_doms_df.loc[top_doms_df['domain'].str.startswith('airflow')][['domain', 'rank']].reset_index(drop=True))\n",
    "        air = f'there are domains similar to airflow.com:{dict(any_air.values)}'\n",
    "    with open('air.txt', 'w') as f:\n",
    "        f.write(str(air))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "metadata": {},
   "outputs": [],
   "source": [
    "def aalutsuk_print_data(df): \n",
    "    \n",
    "    with open('top_10_domain.csv', 'r') as f:\n",
    "        top_10_domain = f.read()\n",
    "    with open('max_len.txt', 'r') as f:\n",
    "        max_len = f.read()\n",
    "    with open('air.txt', 'r') as f:\n",
    "        air = f.read()\n",
    "        \n",
    "    date = ds\n",
    "\n",
    "    print(f'Top 10 domain: {date}')\n",
    "    print(top_10_domain)\n",
    "    \n",
    "    print(f'Domain with the longest name: {date}')\n",
    "    print(max_len)\n",
    "    \n",
    "    print(f'Airflow rank: {date}')\n",
    "    print(air)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<Task(PythonOperator): aalutsuk_print_data>"
      ]
     },
     "execution_count": 8,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "default_args = {\n",
    "    'owner': 'a-lutsuk-20',\n",
    "    'depends_on_past': False,\n",
    "    'retries': 2,\n",
    "    'retry_delay': timedelta(minutes=5),\n",
    "    'start_date': datetime(2022, 7, 31),\n",
    "}\n",
    "schedule_interval = '0 10 * * *'\n",
    "\n",
    "dag = DAG('a-lutsuk-20_2_lesson', default_args=default_args, schedule_interval=schedule_interval)\n",
    "\n",
    "t1 = PythonOperator(task_id='aalutsuk_get_data',\n",
    "                    python_callable=aalutsuk_get_data,\n",
    "                    dag=dag)\n",
    "\n",
    "t2 = PythonOperator(task_id='aalutsuk_top_10_domain',\n",
    "                    python_callable=aalutsuk_top_10_domain,\n",
    "                    dag=dag)\n",
    "\n",
    "t3 = PythonOperator(task_id='aalutsuk_max_length',\n",
    "                        python_callable=aalutsuk_max_length,\n",
    "                        dag=dag)\n",
    "\n",
    "t4 = PythonOperator(task_id='aalutsuk_airflow',\n",
    "                        python_callable=aalutsuk_airflow,\n",
    "                        dag=dag)\n",
    "\n",
    "t5 = PythonOperator(task_id='aalutsuk_print_data',\n",
    "                    python_callable=aalutsuk_print_data,\n",
    "                    dag=dag)\n",
    "\n",
    "t1 >> [t2, t3, t4] >> t5\n",
    "\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.3"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
