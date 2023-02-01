import sys
from os.path import join as pjoin
import yaml
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.contrib.hooks.vertica_hook import VerticaHook
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.append("/home/mclaw/.airflow/dags/mobility_paper_pipeline") # To avoid dependency problems with scheduler
#sys.path.append(os.path.join(os.path.dirname(__file__), "/mobility_paper_pipeline"))
import mobility as f

# Quick and dirty way to handle private credentials
PROJ_DIR = '/home/mclaw/.airflow/dags/mobility_paper_pipeline'
env = yaml.safe_load(open(pjoin(PROJ_DIR,'secrets.yml')))

with DAG(
    "mobility_data_etl",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    description="ETL pipeline to get clean mobility tracking data",
    schedule_interval=None,
    start_date=datetime.today(),
    catchup=False,
    tags=["example"],
) as dag:
    #schedule=timedelta(days=1),

    clean = BashOperator(
        task_id="clean_data",
        bash_command=\
            "sudo -u dbadmin /opt/anritsu/Hadoop/hadoop-3.3.4/bin/hadoop fs -rm -r -f /apps/spark/mobilitydwh/fact_tb_staging",
    )
    
    etl = PythonOperator(
        task_id='etl', 
        python_callable=f.vertica_etl,
        op_kwargs={'queries_dir': '/home/mclaw/.airflow/dags/mobility_paper_pipeline/mobility/queries'},
    )
        
    set_permissions = BashOperator(
        task_id="set_permissions",
        bash_command=\
            "sudo -u dbadmin /opt/anritsu/Hadoop/hadoop-3.3.4/bin/hadoop fs -chmod -R 777 /apps/spark/mobilitydwh/fact_tb_staging",
    )
        
    merge = PythonOperator(
        task_id='merge', 
        python_callable=f.merge_new_data,
        op_kwargs={'mem_exec_gb': 10, 'hdfs_host' : env['local_hadoop']['host']},
    )
    
    update_data = BashOperator(
        task_id="update_data",
        bash_command=\
            "sudo -u dbadmin /opt/anritsu/Hadoop/hadoop-3.3.4/bin/hadoop fs -rm -r -f /apps/spark/mobilitydwh/fact_tb && sudo -u dbadmin /opt/anritsu/Hadoop/hadoop-3.3.4/bin/hadoop fs -mv /apps/spark/mobilitydwh/fact_tb_updated /apps/spark/mobilitydwh/fact_tb",
    )
    
    show = PythonOperator(
        task_id='show', 
        python_callable=f.display_mobility_data,
        op_kwargs={'mem_exec_gb': 10, 'hdfs_host' : env['local_hadoop']['host']},
    )

    
    clean >> etl >> set_permissions >> merge >> update_data >> show
    