from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
import pathlib
import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="vod_history_based",
    description="vod_history_based recommendation",
    start_date=airflow.utils.dates.days_ago(1),  # start_date가 현재 날짜보다 과거로 설정
    schedule_interval=timedelta(days=1),  # 하루에 한번씩 실행
    catchup=False,  # 과거의 DAG 실행 방지
) as dag:

    t1a = BashOperator(
        task_id='user_train',
        bash_command='python3 /home/ubuntu/airflow/dags/hellody_vod_history_based_airflow/00.user_train_airflow.py',
    )

    t1b = BashOperator(
        task_id='movie_train',
        bash_command='python3 /home/ubuntu/airflow/dags/hellody_vod_history_based_airflow/01.movie_train_airflow.py',
    )

    t1c = BashOperator(
        task_id='hellovision_popular',
        bash_command='python3 /home/ubuntu/airflow/dags/hellody_vod_history_based_airflow/02.popular_movie_airflow.py',
    )

    t1d = BashOperator(
        task_id='movie_recommend',
        bash_command='python3 /home/ubuntu/airflow/dags/hellody_vod_history_based_airflow/03.movie_recommend_airflow.py',
    )
    # 태스크 간의 종속성 설정
    t1a >> t1b >> t1c >> t1d
