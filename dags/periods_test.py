from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
import pendulum

with DAG(
    dag_id = "periods_test_5.3.1",
    start_date = pendulum.today('UTC').add(days=-90),
    description = "This DAG will process rocket data",
    schedule = "45 13 * * 1,3,5",
) as dag:
    procure_fuel = EmptyOperator(task_id = "procure_fuel", dag=dag)
    procure_rocket_material = EmptyOperator(task_id = "procure_rocket_material", dag=dag)
    build_stage_1 = EmptyOperator(task_id = "build_stage_1", dag=dag)
    build_stage_2 = EmptyOperator(task_id = "build_stage_2", dag=dag)
    build_stage_3 = EmptyOperator(task_id = "build_stage_3", dag=dag)
    launch = EmptyOperator(task_id = "launch", dag=dag)

    procure_rocket_material >> [build_stage_1, build_stage_2, build_stage_3] >> launch
    procure_fuel >> build_stage_3
    