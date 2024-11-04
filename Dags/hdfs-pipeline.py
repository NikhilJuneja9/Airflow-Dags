############################################################################################################
""" 
This Airflow DAG transfers a CSV file from a local directory to HDFS. It checks for the required CSV file, 
ensures the Ambari and HDFS services are operational, and then appends the CSV data to a file in HDFS.
"""
############################################################################################################


from airflow import DAG
from datetime import datetime,timedelta
import pytz
from airflow.sensors.filesystem import FileSensor
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator 

AMBARI = 'hadoop.ddns.net:50070'



default_args = {
    "owner":"airflow",
    'depends_on_past': False,
    "email_on_failure":False,
    "retries":1,
    "retry_delay":timedelta(minutes=1),
    'email_on_retry': False,
}


    

with DAG(dag_id = "csv_to_hdfs",
         description="This pipeline will fetch radar data at every 5 mins and store it into HDFS",
         catchup=False,start_date =datetime(2024,10,3),schedule_interval=timedelta(minutes=5),default_args=default_args ) as dag:
        
        # this task trigger the radar_data_simulation dag , make sure radar data simulation dag is active (check airflow Ui for active dags list)
        triggerdag = TriggerDagRunOperator(
            task_id = "triggerdag",
            trigger_dag_id= "radar_data_simulation"
        )
        
        
        # Checks if radar_data.csv is available in the specified directory
        check_csv_exists = FileSensor(
                task_id = "check_csv_exists",
                filepath = f"/home/4px/radar_data/radar_data.csv",
                poke_interval = 5,
                timeout = 60,
        )
        
       # Verifies the status of Ambari; ambari_conn_id should be set up in Airflow connections
        check_ambari_status = HttpSensor(
            task_id = "check_ambari_status",
            http_conn_id="ambari_conn_id",
            method="GET",
            response_check= lambda res : res.status_code in (200,201),
            endpoint="#/main/dashboard/metrics",
            poke_interval = 5,
            timeout = 300,)
        
        # Ensures HDFS is active by pinging WebHDFS API
        check_hdfs_is_up = HttpSensor(
            task_id = "check_hdfs_is_up",
            http_conn_id = "hdfs_conn_id",
            endpoint = "webhdfs/v1/?op=LISTSTATUS&noredirect=true",
            method= "GET",
            response_check= lambda response: response.status_code in (200 ,201),
            poke_interval = 5,
            timeout = 300,
        )
        
        # Appends radar data to the existing file in HDFS at /tmp/MOVE/radar_data.csv
        append_file = BashOperator(
              task_id = "append_file",
              bash_command= "curl -i -X POST 'http://hadoop.ddns.net:50075/webhdfs/v1/tmp/MOVE/radar_data.csv?op=APPEND&namenoderpcaddress=hadoop.ddns.net:8020' -T '/home/4px/radar_data/radar_data.csv'"
        )
        

        # Remove the local radar_data.csv file after successful upload to HDFS.
        remove_file_when_loaded = BashOperator(
            task_id = "remove_file",
            bash_command = "rm /home/4px/radar_data/radar_data.csv"
        )
        
        # Define the task execution order.
        triggerdag >> check_csv_exists >> check_ambari_status>>check_hdfs_is_up >> append_file >> remove_file_when_loaded