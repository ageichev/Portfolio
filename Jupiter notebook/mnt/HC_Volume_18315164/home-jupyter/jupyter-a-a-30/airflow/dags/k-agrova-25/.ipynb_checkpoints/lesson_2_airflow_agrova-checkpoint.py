{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "#### Импортируем библиотеки"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
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
    "from airflow.operators.python import PythonOperator"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "#### Подгружаем данные"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "metadata": {},
   "outputs": [],
   "source": [
    "TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'\n",
    "TOP_1M_DOMAINS_FILE = 'top-1m.csv'"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "#### Таски"
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
    "    top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.ru')]\n",
    "    top_data_top_10 = top_data_top_10.head(10)\n",
    "    with open('top_data_top_10.csv', 'w') as f:\n",
    "        f.write(top_data_top_10.to_csv(index=False, header=False))\n",
    "\n",
    "\n",
    "def get_stat_com():\n",
    "    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])\n",
    "    top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.com')]\n",
    "    top_data_top_10 = top_data_top_10.head(10)\n",
    "    with open('top_data_top_10_com.csv', 'w') as f:\n",
    "        f.write(top_data_top_10.to_csv(index=False, header=False))\n",
    "\n",
    "\n",
    "def print_data(ds): # передаем глобальную переменную airflow\n",
    "    with open('top_data_top_10.csv', 'r') as f:\n",
    "        all_data = f.read()\n",
    "    with open('top_data_top_10_com.csv', 'r') as f:\n",
    "        all_data_com = f.read()\n",
    "    date = ds\n",
    "\n",
    "    print(f'Top domains in .RU for date {date}')\n",
    "    print(all_data)\n",
    "\n",
    "    print(f'Top domains in .COM for date {date}')\n",
    "    print(all_data_com)\n",
    "\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "#### Инициализируем DAG"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 16,
   "metadata": {},
   "outputs": [],
   "source": [
    "default_args = {\n",
    "    'owner': 'k.agrova',\n",
    "    'depends_on_past': False,\n",
    "    'retries': 2,\n",
    "    'retry_delay': timedelta(minutes=2),\n",
    "    'start_date': datetime(2023, 1, 11),\n",
    "    'schedule_interval': '5 1 * * *'\n",
    "}\n",
    "dag_agrova = DAG('k_agrova', default_args=default_args)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "#### Инициализируем таски"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 17,
   "metadata": {},
   "outputs": [],
   "source": [
    "t1 = PythonOperator(task_id='get_data',\n",
    "                    python_callable=get_data,\n",
    "                    dag=dag_agrova)\n",
    "\n",
    "t2 = PythonOperator(task_id='get_stat',\n",
    "                    python_callable=get_stat,\n",
    "                    dag=dag_agrova)\n",
    "\n",
    "t2_com = PythonOperator(task_id='get_stat_com',\n",
    "                        python_callable=get_stat_com,\n",
    "                        dag=dag_agrova)\n",
    "\n",
    "t3 = PythonOperator(task_id='print_data',\n",
    "                    python_callable=print_data,\n",
    "                    dag=dag_agrova)\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "#### Задаем порядок выполнения"
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
    "t1 >> [t2, t2_com] >> t3"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 22,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<function __main__.get_data()>"
      ]
     },
     "execution_count": 22,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "get_data"
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
