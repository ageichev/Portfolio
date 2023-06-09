{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "d8e65514",
   "metadata": {},
   "outputs": [],
   "source": [
    "import requests\n",
    "from zipfile import ZipFile\n",
    "from io import BytesIO\n",
    "import pandas as pd\n",
    "from datetime import timedelta\n",
    "from datetime import datetime\n",
    "from airflow import DAG\n",
    "from airflow.operators.python import PythonOperator\n",
    "\n",
    "\n",
    "TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'\n",
    "TOP_1M_DOMAINS_FILE = 'top-1m.csv'\n",
    "\n",
    "\n",
    "\n",
    "\n",
    "\n",
    "# Таски\n",
    "def get_data():\n",
    "    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)\n",
    "    zipfile = ZipFile(BytesIO(top_doms.content))\n",
    "    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')\n",
    "\n",
    "    with open(TOP_1M_DOMAINS_FILE, 'w') as f:\n",
    "        f.write(top_data)\n",
    "\n",
    "\n",
    "def get_top_10_domain_zone():\n",
    "    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])\n",
    "    top_10 = top_data_df\n",
    "    top_10['domain_zone'] = top_10.domain.str.split('.').str[-1]\n",
    "    top_10.groupby('domain_zone', as_index=False).agg({'rank': 'count'}).sort_values('rank', ascending=False).head(10)\n",
    "    with open('top_10_domain_zone.csv', 'w') as f:\n",
    "        f.write(top_10_domain_zone.to_csv(index=False, header=False))\n",
    "\n",
    "\n",
    "def get_top_len_name():\n",
    "    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])\n",
    "    top_len_name = top_data_df\n",
    "    top_len_name['len_name'] = top_len_name.domain.str.split('.').str[0].apply(lambda x:len(x))\n",
    "    top_len_name.sort_values(['len_name', 'domain'], ascending=[False, True]).head(1)['domain']\n",
    "    with open('top_len_name.csv', 'w') as f:\n",
    "        f.write(top_len_name.to_csv(index=False, header=False))\n",
    "        \n",
    "\n",
    "def get_airflow_place():\n",
    "    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])\n",
    "    airflow_place = top_data_df.query(\"domain == 'airflow.com'\")['rank']\n",
    "    with open('airflow_place.csv', 'w') as f:\n",
    "        f.write(airflow_place.to_csv(index=False, header=False))\n",
    "        \n",
    "        \n",
    "def print_data(ds): # передаем глобальную переменную airflow\n",
    "    with open('top_10_domain_zone.csv', 'r') as f:\n",
    "        top_10_domain_zone = f.read()\n",
    "    with open('top_len_name.csv', 'r') as f:\n",
    "        top_len_name = f.read()\n",
    "    with open('airflow_place.csv', 'r') as f:\n",
    "        airflow_place = f.read()        \n",
    "    date = ds\n",
    "\n",
    "    print(f'top_10_domain_zone for date {date}')\n",
    "    print(top_10_domain_zone)\n",
    "\n",
    "    print(f'top_len_name for date {date}')\n",
    "    print(top_len_name)\n",
    "\n",
    "    print(f'airflow_place for date {date}')\n",
    "    print(airflow_place)\n",
    "\n",
    "\n",
    "    \n",
    "    \n",
    "# Инициализируем DAG\n",
    "default_args = {\n",
    "    'owner': 'n.kulibaba',\n",
    "    'depends_on_past': False,\n",
    "    'retries': 2,\n",
    "    'retry_delay': timedelta(minutes=5),\n",
    "    'start_date': datetime(2023, 2, 13),\n",
    "    'schedule_interval': '0 24 * * *'\n",
    "}\n",
    "dag = DAG('n-kulibaba', default_args=default_args)\n",
    "\n",
    "\n",
    "\n",
    "\n",
    "# Инициализируем таски\n",
    "t1 = PythonOperator(task_id='get_data',\n",
    "                    python_callable=get_data,\n",
    "                    dag=dag)\n",
    "\n",
    "t2_1 = PythonOperator(task_id='get_top_10_domain_zone',\n",
    "                    python_callable=get_top_10_domain_zone,\n",
    "                    dag=dag)\n",
    "\n",
    "t2_2 = PythonOperator(task_id='get_top_len_name',\n",
    "                        python_callable=get_top_len_name,\n",
    "                        dag=dag)\n",
    "\n",
    "t2_3 = PythonOperator(task_id='get_airflow_place',\n",
    "                        python_callable=get_airflow_place,\n",
    "                        dag=dag)\n",
    "\n",
    "t3 = PythonOperator(task_id='print_data',\n",
    "                    python_callable=print_data,\n",
    "                    dag=dag)\n",
    "\n",
    "\n",
    "\n",
    "# Задаем порядок выполнения\n",
    "t1 >> t2_1 >> t2_2 >> t2_3 >> t3"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
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
   "version": "3.9.12"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
