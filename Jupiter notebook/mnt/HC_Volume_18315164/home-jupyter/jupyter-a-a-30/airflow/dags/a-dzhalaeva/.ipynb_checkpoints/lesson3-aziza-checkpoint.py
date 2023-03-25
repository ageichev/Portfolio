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
    "import numpy as np\n",
    "from datetime import timedelta\n",
    "from datetime import datetime\n",
    "from io import StringIO\n",
    "\n",
    "from airflow.decorators import dag, task\n",
    "from airflow.operators.python import get_current_context\n",
    "from airflow.models import Variable"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {},
   "outputs": [],
   "source": [
    "path = 'vgsales.csv' \n",
    "login = 'a-dzhalaeva'\n",
    "year = 1994 + hash(f'{login}') % 23\n",
    "\n",
    "CHAT_ID = -620798068\n",
    "try:\n",
    "    BOT_TOKEN = Variable.get('telegram_secret')\n",
    "except:\n",
    "    BOT_TOKEN = ''"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "metadata": {},
   "outputs": [],
   "source": [
    "#Пишем функцию которая будет отправлять сообщения в чат телеграм\n",
    "def send_message(context):\n",
    "    date = context['ds']\n",
    "    dag_id = context['dag'].dag_id\n",
    "    message = f'Very good! Dag {dag_id} is completed on {date}.'\n",
    "    params = {'chat_id': CHAT_ID, 'text': message}\n",
    "    base_url = f'https://api.telegram.org/bot{BOT_TOKEN}/'\n",
    "    url = base_url + 'sendMessage?' + urlencode(params)\n",
    "    resp = requests.get(url)\n",
    "                     \n",
    "default_args = {\n",
    "    'owner': 'a.dzhalaeva',\n",
    "    'depends_on_past': False,\n",
    "    'retries': 2,\n",
    "    'retry_delay': timedelta(minutes=5),\n",
    "    'start_date': datetime(2023, 1, 1),\n",
    "    'schedule_interval' : '0 7 * * *',\n",
    "}"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "metadata": {},
   "outputs": [],
   "source": [
    "def answers():\n",
    "                     \n",
    "    #Считали и вернули таблицу\n",
    "    @task(retries=3)\n",
    "    def get_data():\n",
    "        df = pd.read_csv(path)\n",
    "        df = df.query(\"Year == @year\")\n",
    "        return df \n",
    "    "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "metadata": {},
   "outputs": [],
   "source": [
    "@dag(default_args=default_args, catchup=False)\n",
    "def answers():\n",
    "    @task(retries=3)\n",
    "# Cчитываем таблицу\n",
    "    def get_data():\n",
    "        df = pd.read_csv(path)\n",
    "        df = df.query(\"Year == @year\")\n",
    "        return df \n",
    "# Какая игра была самой продаваемой в этом году во всем мире?   \n",
    "    @task(retries=4, retry_delay=timedelta(10))\n",
    "    def top_game(df):\n",
    "        top_game_is = df.groupby('Name', as_index=False) \\\n",
    "        .agg({'Global_Sales':'sum'}) \\\n",
    "        .sort_values(by='Total_Sales', ascending=False) \\\n",
    "        .max()\n",
    "        return top_game_is\n",
    "# Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько\n",
    "    @task()\n",
    "    def top_in_EU(df):\n",
    "        top_in_EU_is = df.groupby('Genre', as_index=False) \\\n",
    "        .agg({'EU_Sales':'sum'}) \\\n",
    "        .sort_values(by='EU_Sales', ascending=False) \\\n",
    "        .max()\n",
    "        return top_in_EU_is\n",
    "# На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?\n",
    "# Перечислить все, если их несколько\n",
    "    @task()\n",
    "    def top_NA_Sales(df):\n",
    "        NA_Sales_million_is = df.query('NA_Sales > 1') \\\n",
    "        .groupby([\"Platform\"], as_index=False) \\\n",
    "        .agg({\"NA_Sales\":\"sum\"}) \\\n",
    "        .sort_values(\"NA_Sales\", ascending=False) \\\n",
    "        .max()\n",
    "        return NA_Sales_million_is\n",
    "# У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько\n",
    "    @task()\n",
    "    def JP_top_sales(df):\n",
    "        JP_pablisher_top__is = df.groupby([\"Publisher\"], as_index=False) \\\n",
    "        .agg({\"JP_Sales\":\"sum\"}) \\\n",
    "        .sort_values(\"JP_Sales\", ascending=False)\\\n",
    "        .max() \n",
    "        return JP_pablisher_top__is\n",
    "#  Сколько игр продались лучше в Европе, чем в Японии?\n",
    "    @task()\n",
    "    def EU_vs_JP(df):\n",
    "        vs_top = df.groupby([\"Name\"], as_index=False) \\\n",
    "        .agg({\"JP_Sales\":\"sum\", \"EU_Sales\":\"sum\"}) \\\n",
    "        .query('EU_Sales > JP_Sales') \\\n",
    "        .shape[0]\n",
    "        return vs_top\n",
    "\n",
    "    @task(on_success_callback=send_message)\n",
    "    def print_data(top_game_is, top_in_EU_is, NA_Sales_million_is, JP_pablisher_top__is, vs_top):\n",
    "\n",
    "        context = get_current_context()\n",
    "        date = context['ds']\n",
    "\n",
    "        print(f' Какая игра была самой продаваемой в {date}г. во всем мире?')\n",
    "        print(top_game_is)\n",
    "        print(f' Игры какого жанра были самыми продаваемыми в {date}г. в Европе?')\n",
    "        print(top_in_EU_is)\n",
    "        print(f' На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в {date}г. в Северной Америке?')\n",
    "        print(NA_Sales_million_is)\n",
    "        print(f' У какого издателя самые высокие средние продажи в Японии за {date}г.?')\n",
    "        print(JP_pablisher_top__is)\n",
    "        print(f' Сколько игр продались лучше в Европе, чем в Японии за {date}г.?')\n",
    "        print(vs_top)\n",
    "\n",
    "    top_game_is = top_game(df)\n",
    "    top_in_EU_is = top_in_EU(df)\n",
    "    NA_Sales_million_is = top_NA_Sales(df)\n",
    "    JP_pablisher_top__is = JP_top_sales(df)\n",
    "    vs_top = EU_vs_JP(df)\n",
    "    \n",
    "    print_data(top_game_is, top_in_EU_is, NA_Sales_million_is, JP_pablisher_top__is, vs_top)\n",
    "    \n",
    "dag_answers_for_task = answers()\n"
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