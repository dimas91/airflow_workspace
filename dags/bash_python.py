from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum

with DAG(
    dag_id = "bash_python",
    start_date = pendulum.today('UTC').add(days=-1),
    description = "This DAG will print context stuff",
    schedule = "@daily",
) as dag:
    hello = BashOperator(
        task_id = "print_bash",
        bash_command = "echo '{{ task }} is running in the {{ dag }} pipeline'"
    )

    def _print_exec_date_and_days_after(**context):
        exec_date = context["templates_dict"]["execution_date"]
        three_days_after_exec_date = pendulum.parse(exec_date).add(days=3)
        script_run_date = context["templates_dict"]["script_run_date"]
        print(f"The script was executed at {exec_date}")
        print(f"Three days after execution is {three_days_after_exec_date}")
        print(f"This script run date is {script_run_date}")
        return

    world = PythonOperator(
        task_id = "print_python",
        python_callable = _print_exec_date_and_days_after,
        templates_dict = {
            "execution_date": "{{ execution_date }}",
            "script_run_date": "{{ ds }}"
        }
    )

    hello >> world