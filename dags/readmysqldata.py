from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Function to query sql database 
def query_mysql(**kwargs):
    mysql_hook=MySqlHook(mysql_conn_id='my_local_mysql')

    connection=mysql_hook.get_conn()
    cursor=connection.cursor()

    cursor.execute("select * from product_staging_table")
    rows=cursor.fetchall()

    for row in rows:
        print(row)
    
    cursor.close()
    connection.close()

dag=DAG('mysql_connection_example',
        description='a simple example for mysql connection',
        schedule_interval=None,
        start_date=datetime(2024,12,27),
        catchup=False
        )
mysql_task=PythonOperator(
    task_id='query_mysql_task',
    python_callable=query_mysql,
    provide_context=True,
    dag=dag)
