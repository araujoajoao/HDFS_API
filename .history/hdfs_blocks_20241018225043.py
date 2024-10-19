import os
import time
from datetime import datetime, timedelta
import logging
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.session import create_session
from airflow.models.dag import get_last_dagrun
from airflow.utils.task_group import TaskGroup
from custom_functions.FindLogErrors import FindLogErrors

epoch = datetime.now().strftime("%s")

# Variables
NAMENODE = Variable.get("NAMENODE_HOST_ID")
API_USER = Variable.get("API_PASSWORD")
API_PASSWORD = Variable.get("API_PASSWORD")

# Project path
bucket_path = Variable.get("BUCKET_INTERMEDIARY")
project_path = "gs://{bucket_path}/coe_automacoes".format(bucket_path=bucket_path)

# Create object to send email
error_email = FindLogErrors(
    email="mine@fakemail.com",
    urgency=0,
    tags=[5],
    bitbucket_repo="repo_id",
    sla="2 Horas",
    emails_to_send=[
        "mine@fakemail.com",
    ],
    email_copy="mine@fakemail.com",
)

# Default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_retry": False,
    "email_on_failure": False,
    "on_failure_callback": error_email.buildEmail,
}


def check_hour(hour=12, minute=2):
    """Check if current time matches the specified hour and minute."""
    current_time = datetime.now()
    return (
        upscale_group
        if current_time.hour == hour and current_time.minute == minute
        else task_default
    )


def get_execution_date_nivel_risco(exec_date, **kwargs):
    with create_session() as session:
        dag_a_last_run = get_last_dagrun(
            dag_id="dag_id", task_id="task_id", session=session
        )
        return dag_a_last_run.execution_date if dag_a_last_run else None


def get_last_under_replicated_blocks():
    # Fetch last under-replicated blocks data from the CM API.
    url = os.path.join(NAMENODE, "cdp-proxy-api/cm-api/v51/timeseries")
    query = (
        "select under_replicated_blocks_across_hdfss "
        "WHERE clusterName = 'cluster_id'}"
    )

    current_time = int(time.time() * 1000)
    start_time = current_time - 3600000

    params = {
        "query": query,
        "startTime": str(start_time),
        "endTime": str(current_time),
    }

    try:
        response = requests.get(
            url,
            params=params,
            auth=(API_USER, API_PASSWORD),
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        items = data.get("items", [])
        if items:
            time_series = items[0].get("timeSeries", [])
            if time_series:
                data_points = time_series[0].get("data", [])
                if data_points:
                    last_value = data_points[-1].get("value")
                    logging.info(f"Under-replicated blocks: {last_value}")
                    return last_value
        logging.warning("No data points found.")

    except requests.RequestException as e:
        logging.error(f"API connection error: {e}")

    return None


def check_and_scale(**kwargs):
    """
    Check under-replicated blocks and decide the next task:
    - Downscale if blocks are zero.
    - No action otherwise.
    """
    last_value = get_last_under_replicated_blocks()

    if last_value == 0:
        logging.info("Under-replicated blocks are zero. Initiating downscale.")
        return downscale_group

    logging.info("Under-replicated blocks are not zero. No scaling action.")
    return task_default


# DAG definition
with DAG(
    "DAG Name",
    start_date=datetime(1900, 1, 10),
    default_args=default_args,
    schedule_interval="2 12 1,2,3,4,5,6 * *",
    catchup=False,
    max_active_runs=1,
    tags=["PRODUCTION"],
) as dag:
    check_hour = BranchPythonOperator(
        task_id="check_hour",
        python_callable=check_hour,
    )

    # TaskGroup for Upscaling Cluster
    with TaskGroup("upscale_cluster") as upscale_group:
        upscale_30_nodes = SSHOperator(
            task_id="upscale_30_nodes",
            ssh_conn_id="test-ssh",
            command=(
                "cluster command"
            ),
        )

    # External task sensor for IFRS9_90_DIAS before downscaling
    ifrs9_sensor = ExternalTaskSensor(
        task_id="rtask_id",
        external_dag_id="IFRS9_90_DIAS",
        external_task_id="t_regra_de_controle",
        allowed_states=["success"],
        mode="poke",
        timeout=1800,
    )

    get_hdfs_data = PythonOperator(
        task_id="get_hdfs_blocks",
        python_callable=get_last_under_replicated_blocks,
    )

    # TaskGroup for downscaling cluster with HDFS checks between
    with TaskGroup("downscale_cluster") as downscale_group:
        previous_check_task = None
        downscale_nodes = [14, 8, 5, 3]
        for i, nodes in enumerate(downscale_nodes, start=1):
            downscale_task = SSHOperator(
                task_id=f"downscale_{nodes}_nodes",
                ssh_conn_id="test-ssh",
                command=(
                    "cdp datahub scale-cluster --cluster-name di-pnb-test-spark3 "
                    f"--instance-group-name worker --instance-group-desired-count {nodes}"
                ),
            )
            check_task = PythonOperator(
                task_id=f"check_blocks_{nodes}",
                python_callable=get_last_under_replicated_blocks,
            )
            if previous_check_task is not None:
                previous_check_task >> downscale_task
            downscale_task >> check_task
            previous_check_task = check_task

    task_default = EmptyOperator(task_id="downscale_success")

    # Task dependencies
    check_hour >> upscale_group >> ifrs9_sensor
    ifrs9_sensor >> get_hdfs_data >> downscale_group >> task_default