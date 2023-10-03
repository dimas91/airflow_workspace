from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum

with DAG(
    dag_id = "hello_world",
    start_date = pendulum.today('UTC').add(days=-14),
    description = "This DAG will print \"Hello\" & \"World\"",
    schedule = "@daily",
) as dag:
    hello = BashOperator(
        task_id = "hello",
        bash_command = "echo 'hello'"
    )

    world = PythonOperator(
        task_id = "world",
        python_callable = lambda: print("world")
    )

    hello >> world