from datetime import datetime, timedelta
from airflow.models import DAG, BaseOperator

import sys
print(sys.executable)

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator

from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.providers.microsoft.azure.sensors.data_factory import AzureDataFactoryPipelineRunStatusSensor
from airflow.utils.edgemodifier import Label

with DAG(dag_id = "example_adf_run_pipeline",
         start_date = datetime(2023,8,19),
         schedule_interval = "*/5 * * * *",
         catchup = False,
         default_args = {
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
        "azure_data_factory_conn_id": "airflow-adf-001", #This is a connection created on Airflow UI
        "factory_name": "adf-001",  # This can also be specified in the ADF connection.
        "resource_group_name": "rg-001",  # This can also be specified in the ADF connection.
    },
    default_view="graph",) as dag:
        begin = EmptyOperator(task_id = "begin")
        end = EmptyOperator(task_id = "end")

        run_pipeline1: BaseOperator = AzureDataFactoryRunPipelineOperator(
             task_id = "run_pipeline1",
             pipeline_name = "test_variables",
             parameters = {"sourceSubSystem":"API"},
        )

        begin >> run_pipeline1 >> end
