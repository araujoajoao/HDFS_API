# HDFS_API

This DAG fetches the last under-replicated blocks data from CDP CM Time Series API and prints the last timestamp and number of under-replicated blocks.

The DAG is scheduled to run daily at 2:00 AM.

The DAG uses the following variables:
- `cm-host`: The hostname of the Cloudera Manager cluster.
- `cluster-name`: The name of the cluster.
- `namespace`: The namespace for the API.
- `username`: The username for the API.
- `password`: The password for the API.

Cloudera Manager Time Series API Docs: https://docs.cloudera.com/documentation/enterprise/latest/topics/cm_dg_tsquery.html#cmug_topic_11_7_1