from airflow import DAG
from datetime import datetime
import os
import sys
from airflow.operators.python import PythonOperator
import logging
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.wikipedia_pipeline import extract_wikipedia_data, transform_wikipedia_data, write_wikipedia_data

def test_logging(**context):
    """Simple test function to verify logging is working"""
    logging.info("Starting test_logging function")
    print("Print statement from test_logging")
    logging.warning("This is a warning message")
    logging.error("This is an error message (not a real error)")
    logging.info("Test logging complete")
    return "Test logging successful"

dag = DAG(    
    dag_id="wikipedia_flow",
    default_args={
        "owner": "Abderrahmen Bejaoui",
        "start_date": datetime(2023, 10, 1),
        "schedule_interval": None,
    },
    schedule_interval=None,
    catchup=False,
)

#Test logging task
test_logging_task = PythonOperator(
    task_id="test_logging",
    python_callable=test_logging,
    provide_context=True,
    dag=dag,
)

#Extraction
extract_data_from_wikipedia = PythonOperator(
    task_id="extract_data_from_wikipedia",
    python_callable=extract_wikipedia_data,
    provide_context=True,
    op_kwargs={"url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},
    dag=dag,
)

transform_data_from_wikipedia = PythonOperator(
    task_id="transform_wikipedia_data",
    python_callable=transform_wikipedia_data,
    provide_context=True,
    dag=dag,
)

#write

write_wikipedia_data = PythonOperator(
    task_id="write_wikipedia_data",
    python_callable=write_wikipedia_data,
    provide_context=True,
    dag=dag,
)

test_logging_task >> extract_data_from_wikipedia >> transform_data_from_wikipedia >> write_wikipedia_data