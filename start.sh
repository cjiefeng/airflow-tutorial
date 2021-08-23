export AIRFLOW_HOME=$(pwd)/airflow
airflow webserver --daemon & airflow scheduler