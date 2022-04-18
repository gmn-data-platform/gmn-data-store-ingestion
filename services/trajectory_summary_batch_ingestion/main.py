"""Airflow script to produce trajectory summary data."""
import json
import tempfile
from datetime import timedelta, datetime
import os

from airflow.operators.python import get_current_context, PythonOperator
from airflow.utils.dates import days_ago
from airflow.decorators import dag

import confluent_kafka.avro
from confluent_kafka.avro import AvroProducer

from gmn_python_api.data_directory import DATA_START_DATE
from gmn_python_api.data_directory import get_daily_file_content_by_date
from gmn_python_api.trajectory_summary_reader import \
    read_trajectory_summary_as_dataframe
from gmn_python_api.trajectory_summary_schema import get_trajectory_summary_avro_schema, \
    _MODEL_TRAJECTORY_SUMMARY_FILE_PATH
from gmn_python_api import get_all_file_content
import gmn_python_api

EXTRACTED_DATA_DIRECTORY = '~/extracted_data'
"""Directory that stores raw trajectory summary files after getting them using the
 gmn-python-api."""

TRAJECTORY_SUMMARY_TOPIC_NAME = 'trajectory_summary_raw'
"""The kafka topics to produce trajectory summary messages to."""


def save_extracted_daily_trajectory_summary_file(extracted_data_directory: str,
                                                 target_date: datetime,
                                                 logical_date: datetime,
                                                 execution_date: datetime,
                                                 file_content: str) -> str:
    """
    Save file_content in the daily extracted directory.
    :param extracted_data_directory: The name of the directory to create within the
     extracted directory.
    :param target_date: The trajectory summary file date to save.
    :param logical_date: The Airflow logical date.
    :param execution_date: The Airflow execution date.
    :param file_content: The contents of the trajectory file to save.
    :return: The path of the created file.
    """
    execution_date_extracted_data_directory = os.path.join(
        os.path.expanduser(extracted_data_directory),
        execution_date.strftime('%Y%m%d'))
    os.makedirs(execution_date_extracted_data_directory, exist_ok=True)
    filename = f"target_{target_date}_logical_{logical_date}" \
               f"_execution_{execution_date}.log"
    file_path = os.path.join(execution_date_extracted_data_directory, filename)
    file = open(file_path, "w")
    file.write(file_content)
    file.close()

    return file_path


def save_all_trajectory_summary_file(extracted_data_directory: str,
                                     execution_date: datetime,
                                     file_content: str) -> str:
    """
    Save the file contents of the extracted all trajectory summary file in the
    extracted data directory.
    :param extracted_data_directory: The name of the directory to create within the
     extracted directory.
    :param execution_date: The Airflow execution date.
    :param file_content: The contents of the trajectory summary file to save.
    :return: The path of the created file.
    """
    execution_date_extracted_data_directory = os.path.join(
        os.path.expanduser(extracted_data_directory),
        execution_date.strftime('%Y%m%d') + "_historical")
    os.makedirs(execution_date_extracted_data_directory, exist_ok=True)
    filename = f"execution_{execution_date.strftime('%Y%m%d')}_historical.log"
    file_path = os.path.join(execution_date_extracted_data_directory, filename)
    file = open(file_path, "w")
    file.write(file_content)
    file.close()

    return file_path


def delivery_trajectory_summary(err, msg) -> None:
    """
    Called once for each message produced to indicate delivery result. Triggered by
     poll() or flush().
    :param err: Error, if one has occurred delivering the message.
    :param msg: Message object.
    :return: None
    """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def get_avro_schema():
    # TODO: replace with this fix
    #  https://github.com/gmn-data-platform/gmn-python-api/issues/115
    import pandavro as pdx  # type: ignore
    from avro.datafile import DataFileReader  # type: ignore
    from avro.io import DatumReader  # type: ignore

    _, avro_file_path = tempfile.mkstemp()

    data_frame = gmn_python_api.read_trajectory_summary_as_dataframe(
        _MODEL_TRAJECTORY_SUMMARY_FILE_PATH,
        avro_compatible=True,
        avro_long_beginning_utc_time=False,
    )

    pdx.to_avro(avro_file_path, data_frame)
    pdx.read_avro(avro_file_path)

    reader = DataFileReader(open(avro_file_path, "rb"), DatumReader())
    schema = json.loads(reader.meta["avro.schema"].decode())
    return confluent_kafka.avro.loads(json.dumps(dict(schema)))


def trajectory_summary_daily_ingest(day_offset: int = 0) -> None:
    """
    Load a trajectory summary file from the data directory into kafka using the
     gmn-python-api according to the current day minus the day offset.
    :param day_offset: The number of days to offset the current day by.
    :return: None.
    """
    context = get_current_context()
    print(f"Current day {context['logical_date']} Day offset {day_offset}")
    target_date = context['logical_date'] + timedelta(days=day_offset)
    print(
        f"Extracting daily data from {target_date} in dag run with logical date "
        f"{context['logical_date']} "
        f"on current execution date {context['execution_date']}"
        f" day_offset {day_offset}")

    # Find target_date filename by looking at the directory listing
    file_content = get_daily_file_content_by_date(target_date,
                                                  context['execution_date'])

    # Write file to ~/extracted_date/{execution_date}/
    extracted_file_path = save_extracted_daily_trajectory_summary_file(
        EXTRACTED_DATA_DIRECTORY,
        target_date,
        context['logical_date'],
        context['execution_date'],
        file_content)

    trajectory_df = read_trajectory_summary_as_dataframe(extracted_file_path,
                                                         avro_compatible=True)
    print(f"Shape of the data = {trajectory_df.shape}\n")

    avro_schema = get_avro_schema()

    avroProducer = AvroProducer({
        'bootstrap.servers': 'kafka-broker:29092',
        'on_delivery': delivery_trajectory_summary,
        'schema.registry.url': 'http://schema-registry:8081'
    }, default_value_schema=avro_schema)

    for index, row in trajectory_df.iterrows():
        row_dict = dict(row.to_dict())
        print(f"Sending index {index}, row = {row_dict} to kafka")
        avroProducer.produce(topic=TRAJECTORY_SUMMARY_TOPIC_NAME, value=row_dict,
                             key=None)
        avroProducer.poll(0)
        print(f"Successfully sent index {index}")
    avroProducer.flush()


def trajectory_summary_historical_ingest() -> None:
    """
    Load the all trajectory summary file from the data directory into kafka using the
     gmn-python-api.
    :return: None.
    """
    context = get_current_context()
    print(
        f"Historically extracting data from traj_summary_all.txt "
        f"execution date {context['execution_date']}")

    # Find target_date filename by looking at the directory listing
    file_content = get_all_file_content()

    # Write file to ~/extracted_date/{execution_date}/
    extracted_file_path = save_all_trajectory_summary_file(
        EXTRACTED_DATA_DIRECTORY,
        context['execution_date'],
        file_content)

    trajectory_df = read_trajectory_summary_as_dataframe(extracted_file_path,
                                                         avro_compatible=True)
    print(f"Shape of the data = {trajectory_df.shape}\n")

    avroProducer = AvroProducer({
        'bootstrap.servers': 'kafka-broker:29092',
        'on_delivery': delivery_trajectory_summary,
        'schema.registry.url': 'http://schema-registry:8081'
    }, default_key_schema=None,
        default_value_schema=get_trajectory_summary_avro_schema())

    for index, row in trajectory_df.iterrows():
        print(f"Sending index {index}, row = {row.to_dict()} to kafka")
        avroProducer.produce(topic=TRAJECTORY_SUMMARY_TOPIC_NAME, value=row.to_dict(),
                             key=None)
        avroProducer.poll(0)
        print(f"Successfully sent index {index}")
    avroProducer.flush()


# Define common Airflow arguments for the dags
default_args = {
    "owner": "gmndatauser",
    "retires": 5,
    "retry_delay": timedelta(minutes=15),
}


@dag("gmn_data_ingestor_daily_extract_dag", tags=['daily'],
     start_date=DATA_START_DATE, default_args=default_args,
     schedule_interval="0 1 * * *", catchup=False)
def daily_extract_dag() -> None:
    """
    Extract and load the last 10 days of trajectory summary files into kafka in parallel.
    :return: None
    """
    for day_offset in range(-9, 1):
        PythonOperator(
            task_id=f'batch_extract_day.{day_offset}',
            python_callable=trajectory_summary_daily_ingest,
            op_kwargs={'day_offset': day_offset})


@dag("gmn_data_ingestor_historical_extract_dag", tags=['historical'],
     start_date=DATA_START_DATE, default_args=default_args,
     schedule_interval="@once", catchup=False)
def historical_extract_dag() -> None:
    """
    Use the all trajectory summary file to ingest all historical trajectory summary
     data.
    :return: None
    """
    PythonOperator(
        task_id='batch_extract_day_historical',
        python_callable=trajectory_summary_historical_ingest,
        op_kwargs={})


# Make both DAGs global so that Airflow can pick them up
globals()['daily_extract_dag'] = daily_extract_dag()
globals()['historical_extract_dag'] = historical_extract_dag()

# https://airflow.apache.org/docs/apache-airflow/stable/executor/debug.html
if __name__ == "__main__":
    dag = globals()['gmn_data_ingestor_daily_extract_dag']
    dag.clear()
    dag.run(start_date=days_ago(1), end_date=days_ago(0))  # runs yesterdays dag
