# HDFS_API

This project defines an Apache Airflow DAG for monitoring and managing under-replicated HDFS blocks within a cluster environment. The DAG utilizes a mix of custom Python and Airflow operators to control the scale of the cluster based on the availability of under-replicated blocks.

# DAG Flow
Hourly Check: Triggers an hourly task to decide the appropriate action.
Cluster Scaling:
If under-replicated blocks are present, the DAG attempts a series of scaling operations, initially upscaling the cluster.
Once the replication goal is met, it downscales the cluster step-by-step, checking under-replicated blocks at each step.
Task Monitoring: An external task sensor monitors the successful completion of a related DAG before proceeding to downscale tasks.

#Prerequisites
Apache Airflow with dependencies on SSHOperator and ExternalTaskSensor.
Cloudera Manager API access for retrieving HDFS block replication details.
SSH Access to the cluster for executing scaling commands.
Airflow Variables Required:
NAMENODE_HOST_ID: Host address of the NameNode.
API_USER: Username for Cloudera API.
API_PASSWORD: Password for Cloudera API.
BUCKET_INTERMEDIARY: Path to intermediary storage for automation files.

# Project Configuration
The following global variables and default arguments are defined in the DAG:

NAMENODE: Retrieved from the NAMENODE_HOST_ID Airflow variable.
API_USER & API_PASSWORD: Retrieved from Airflow variables for authentication.
error_email: An instance of FindLogErrors for handling error notifications.
Default Arguments: Specifies retry behavior, failure callback, and task dependencies for production execution.
DAG Details
Primary Functions
check_hour(): Branching function that checks the time and routes the workflow accordingly.
get_execution_date_nivel_risco(): Retrieves the latest execution date of a specified task.
get_last_under_replicated_blocks(): Queries Cloudera API for the count of under-replicated blocks in HDFS.
check_and_scale(): Determines scaling action based on the under-replication count.
Task Groups
upscale_cluster: Upscales the cluster by adding nodes.
downscale_cluster: Sequentially downscales the cluster, checking HDFS blocks at each stage.
DAG Schedule
Trigger Time: Runs daily at 12:02 PM on specified days.
Catchup: Disabled to ensure the DAG only runs on the current schedule date.
Error Handling
The FindLogErrors utility is used to handle errors by sending email notifications with detailed logs, supporting swift troubleshooting.

# Running the DAG
Ensure all necessary Airflow variables are set up as described above.
Deploy the DAG by copying it to your Airflow DAG directory.
Start the DAG manually or wait for the scheduled trigger to begin the workflow.
Contributing
Contributions and suggestions are welcome! Please submit issues or pull requests for any improvements or bug fixes.
The DAG is scheduled to run daily at 2:00 AM.

The DAG uses the following variables:
- `cm-host`: The hostname of the Cloudera Manager cluster.
- `cluster-name`: The name of the cluster.
- `namespace`: The namespace for the API.
- `username`: The username for the API.
- `password`: The password for the API.

Cloudera Manager Time Series API Docs: https://docs.cloudera.com/documentation/enterprise/latest/topics/cm_dg_tsquery.html#cmug_topic_11_7_1
