{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {},
   "outputs": [],
   "source": [
    "import requests\n",
    "from zipfile import ZipFile\n",
    "from io import BytesIO\n",
    "import pandas as pd\n",
    "from datetime import timedelta\n",
    "from datetime import datetime\n",
    "\n",
    "from airflow import DAG\n",
    "from airflow.operators.python import PythonOperator\n"
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
   "execution_count": 15,
   "metadata": {},
   "outputs": [],
   "source": [
    "def get_data():\n",
    "    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)\n",
    "    zipfile = ZipFile(BytesIO(top_doms.content))\n",
    "    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')\n",
    "\n",
    "    with open(TOP_1M_DOMAINS_FILE, 'w') as f:\n",
    "        f.write(top_data)\n",
    "\n",
    "\n",
    "def get_stat():\n",
    "    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])\n",
    "    top_data_domen_10 = top_data_df[top_data_df['domain'].str.split('.')[-1]]\n",
    "    top_data_domen_10 = top_data_domen_10.groupby('domain', as_index= False)\\\n",
    "                                        .agg({'rank':'count'})\\\n",
    "                                        .sort_values('rank', ascending = False)\n",
    "    top_data_domen_10 = top_data_domen_10['domain'].head(10)\n",
    "    \n",
    "    with open('top_data_domen_10.csv', 'w') as f:\n",
    "        f.write(top_data_domen_10.to_csv(index=False, header=False))\n",
    "        \n",
    "def get_stat_longest():\n",
    "    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])\n",
    "    longest_domain['domain_length'] = top_data_df['domain'].apply(len)\n",
    "    longest_domain['domain', 'domain_length'].sort_values('domain_length', ascending= False)\\\n",
    "                                            .reset_index()\\\n",
    "                                            .loc[0]['domain']\n",
    "    \n",
    "    with open('longest_domain.csv', 'w') as f:\n",
    "        f.write(longest_domain.to_csv(index=False, header=False))\n",
    "\n",
    "def get_stat_airflow():\n",
    "    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])\n",
    "    \n",
    "    \n",
    "    with open('airflow_rank.csv', 'w') as f:\n",
    "        if top_data_df[top_data_df['domain']== 'airflow.com'].empty:\n",
    "            f.write('airflow.com is not found')\n",
    "        else: \n",
    "            airflow_rank = top_data_df[top_data_df['domain']== 'airflow.com']['rank']\n",
    "            f.write(airflow_rank)\n",
    "\n",
    "    \n",
    "def print_data(ds): # передаем глобальную переменную airflow\n",
    "    \n",
    "    with open('top_data_domen_10.csv', 'r') as f:\n",
    "        domen_data = f.read()\n",
    "    \n",
    "    with open('longest_domain.csv', 'r') as f:\n",
    "        longest_domain_data = f.read()\n",
    "    \n",
    "    with open('airflow_rank', 'r') as f:\n",
    "        airflow_rank_data = f.read()    \n",
    "    date = ds\n",
    "\n",
    "    print(f'Top 10 domain',\"'\",'s regions,\"\\\", {date}')\n",
    "    print(domen_data)\n",
    "\n",
    "    print(f'The longest domain is: {date}')\n",
    "    print(longest_domain_data)\n",
    "          \n",
    "    print(f'Airflow.com rank is: {date}')\n",
    "    print(airflow_rank_data)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 16,
   "metadata": {},
   "outputs": [],
   "source": [
    "default_args = {\n",
    "    'owner': 'di-naumenko',\n",
    "    'depends_on_past': False,\n",
    "    'retries': 2,\n",
    "    'retry_delay': timedelta(minutes=15),\n",
    "    'start_date': datetime(2022, 12, 2),\n",
    "    'schedule_interval': '0 9 * * *'\n",
    "}\n",
    "dag = DAG('di_naumenko', default_args=default_args)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 17,
   "metadata": {},
   "outputs": [],
   "source": [
    "t1 = PythonOperator(task_id='get_data', # Название таска\n",
    "                    python_callable=get_data, # Название функции\n",
    "                    dag=dag) # Параметры DAG\n",
    "\n",
    "t2 = PythonOperator(task_id='get_stat', # Название таска\n",
    "                    python_callable=get_stat, # Название функции\n",
    "                    dag=dag) # Параметры DAG\n",
    "\n",
    "t3 = PythonOperator(task_id='get_stat_longest', # Название таска\n",
    "                    python_callable=get_stat_longest, # Название функции\n",
    "                    dag=dag) # Параметры DAG\n",
    "\n",
    "t4 = PythonOperator(task_id='get_stat_airflow', # Название таска\n",
    "                    python_callable=get_stat_airflow, # Название функции\n",
    "                    dag=dag) # Параметры DAG\n",
    "\n",
    "t5 = PythonOperator(task_id='print_data', # Название таска\n",
    "                    python_callable=print_data, # Название функции\n",
    "                    dag=dag) # Параметры DAG"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 18,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<Task(PythonOperator): print_data>"
      ]
     },
     "execution_count": 18,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "t1 >> [t2, t3, t4] >> t5"
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
