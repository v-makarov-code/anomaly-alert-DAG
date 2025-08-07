import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import telegram
import pandahouse as ph

default_args = {
    'owner': 'v.makarov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 7, 7)
}

class Getch:
    def __init__(self, query, db='simulator'):
        self.connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'dpo_python_2020',
            'user': 'student',
            'database': db,
        }
        self.query = query
        self.getchdf

    @property
    def getchdf(self):
        try:
            self.df = ph.read_clickhouse(self.query, connection=self.connection)

        except Exception as err:
            print("\033[31m {}".format(err))
            exit(0)

def check_anomaly(df, metric, a=4, n=5):
    
    """Метрика ctr распределена нормально поэтому детектирование аномалий 
    реализовано через правило трех сигм
    """
    
    if metric == "ctr":
        
        mu_n = df[metric].shift(1).rolling(n).mean()
        sigma_n = df[metric].shift(1).rolling(n).std()
        
        df["up"] = mu_n + 3*sigma_n
        df["low"] = mu_n - 3*sigma_n
        
        df['up'] = df['up'].rolling(n, center=True, min_periods=3).mean()
        df['low'] = df['low'].rolling(n, center=True, min_periods=3).mean()
        
        if ((df[metric].iloc[-1] < df["low"].iloc[-1])
            or (df[metric].iloc[-1] > df["up"].iloc[-1])):
            is_alert = 1
        else:
            is_alert = 0
     
        return is_alert, df
        
    else:
        df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
        df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
        df['iqr'] = df['q75'] - df['q25']
        
        df['up'] = df['q75'] + a*df['iqr']
        df['up'] = df['up'].rolling(n, center=True, min_periods=3).mean()

        df['low'] = df['q25'] - a*df['iqr']
        df['low'] = df['low'].rolling(n, center=True, min_periods=3).mean()
        
        if ((df[metric].iloc[-1] < df['low'].iloc[-1])
            or (df[metric].iloc[-1] > df['up'].iloc[-1])):
            is_alert = 1
        else:
            is_alert = 0
        
        return is_alert, df   
def run_alerts(chat=None):
    
    chat_id = chat
    my_token = '7244644521:AAHOiP8zKPaNYRkCnnd9uHPWNkZ-fYcis5g'
    bot = telegram.Bot(token=my_token)
    
    data_fa = Getch(''' SELECT
                      toStartOfFifteenMinutes(time) as ts
                    , toDate(ts) as date
                    , formatDateTime(ts, '%R') as hm
                    , uniqExact(user_id) as users_fa
                    , countIf(user_id, action='view') AS views
                    , countIf(user_id, action='like') AS likes
                    , countIf(user_id, action='like')/countIf(user_id, action='view') AS ctr
                FROM  simulator_20250520.feed_actions
                WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                GROUP BY ts, date, hm
                ORDER BY ts ''').df
    data_ma = Getch(''' SELECT
                      toStartOfFifteenMinutes(time) as ts
                    , toDate(ts) as date
                    , formatDateTime(ts, '%R') as hm
                    , uniqExact(user_id) as users_ma
                    , COUNT(user_id) as msgs
                FROM  simulator_20250520.message_actions
                WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                GROUP BY ts, date, hm
                ORDER BY ts ''').df
    data = data_fa.merge(data_ma[["ts", "users_ma", "msgs"]], on="ts")
    
    metric_list = ["users_fa", "views", "likes", "ctr", "users_ma", "msgs"]
    
    for metric in metric_list:
        print(metric)
        df = data[["ts", metric]].copy()
        is_alert, df = check_anomaly(df, metric)
        
        if is_alert == 1:
            
            msg = f"""
🚨Обнаружена аномалия
Метрика: {metric}
Текущее значение: {df[metric].iloc[-1]:.2f}
Отклонение от предыдущего значения: {df[metric].iloc[-1]/df[metric].iloc[-2]-1:.2f}
👉<a href="https://superset.lab.karpov.courses/superset/dashboard/6855/">Подробнее по ссылке на дашборд</a>"""
            
            sns.set(rc={"figure.figsize":(16, 10)})
            plt.tight_layout()
            ax = sns.lineplot(data=df, x='ts', y=metric, label=metric)
            ax = sns.lineplot(data=df, x='ts', y='up', label='up')
            ax = sns.lineplot(data=df, x='ts', y='low', label='low')

            for ind, label in enumerate(ax.get_xticklabels()):
                if ind % 3 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
            ax.set(xlabel='time')
            ax.set(ylabel=metric)
            ax.set_title(metric)
            ax.set(ylim=(0, None))
            
            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = f'{metric} alert.png'
            plt.close()
            
            bot.sendMessage(chat_id=chat_id, text=msg, parse_mode='HTML')
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
            
   
@dag(default_args=default_args, catchup=False, schedule_interval='*/15 * * * *')
def vladislav_makarov_bxs7496_alert():
    @task()
    def make_alert():
        run_alerts(-969316925)
        
    make_alert()
vladislav_makarov_bxs7496_alert = vladislav_makarov_bxs7496_alert()