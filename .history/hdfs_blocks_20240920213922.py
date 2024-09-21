import os
import sys
import time
from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

sys.path.append(os.path.dirname(__file__))

# Change to Variables
namenode_host = "https://{cm-host}/{cluster-name}/{namespace}/"  # Cluster name and namespace
api_user = {"username"}
api_password = {"password"}
# credemntials

default_args = {
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
    "email_on_retry": False,
    "email_on_failure": False,
    "provide_context": True,
    "use_beeline": True,
}


def get_last_under_replicated_blocks():
    """
    Fetch the last under-replicated blocks data from CDP CM Time Series API.

    Returns:
        tuple: Last timestamp and number of under-replicated blocks or None in case of failure.
    """
    # Define the API endpoint and parameters
    url = f"{namenode_host}cdp-proxy-api/cm-api/v51/timeseries"
    query = "select under_replicated_blocks_across_hdfss WHERE clusterName = {'cluster-name'}"
    
    # Current time in milliseconds for dynamic start/end time
    current_time = int(time.time() * 1000)
    start_time = current_time - 3600000  # Subtracting 1 hour for example

    params = {
        "query": query,
        "currentMode": "true",
        "startTime": str(start_time),
        "endTime": str(current_time),
    }
    
    try:
        # Make API call to fetch under-replicated blocks data
        response = requests.get(url, params=params, auth=(api_user, api_password))

        # Check if the response is successful
        if response.status_code == 200:
            data = response.json()
            items = data.get("items", [])
            
            if items:
                # Parse the timeSeries data
                time_series = items[0].get("timeSeries", [])
                
                if time_series:
                    data_points = time_series[0].get("data", [])
                    
                    # Check for data points and extract the last value
                    if data_points:
                        last_point = data_points[-1]
                        last_timestamp = last_point.get("timestamp")
                        last_value = last_point.get("value")
                        print(f"Last Timestamp: {last_timestamp}, Under-replicated blocks: {last_value}")
                        return last_timestamp, last_value
                    else:
                        print("No data points found.")
                else:
                    print("No time series data found.")
            else:
                print("No items found in the response.")
        else:
            print(f"Failed to fetch data. HTTP Status Code: {response.status_code}")
            print(f"Response: {response.text}")
    
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to the API: {e}")
    
    # Return None on error
    return None


# DAG definition
with DAG(
    "hdfs_usage_monitoring",
    start_date=datetime(2024, 9, 16),
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=[{tagName}],
) as dag:

    get_hdfs_data = PythonOperator(
        task_id="get_hdfs_blocks",
        python_callable=get_last_under_replicated_blocks  # Pass function without calling it
    )

    task_default = EmptyOperator(task_id="task_default")


    get_hdfs_data >> task_default