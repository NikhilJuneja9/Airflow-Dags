###################################################################################################################################################
""" 
This Airflow DAG transfers a CSV file from a local directory to vertica table using vertica conn id defined in airflow connections. 
It checks for the required CSV file, ensures the vertica services are operational,create a reqd table in vertica if not exits and 
then adds the CSV data to a vertica table
"""
###################################################################################################################################################


from airflow import DAG
from datetime import datetime,timedelta
from airflow.sensors.filesystem import FileSensor
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from airflow.providers.vertica.hooks.vertica import VerticaHook
import csv



AMBARI ="hadoop.ddns.net:50070" 
PATH = '/home/4px/radar_data/radar_data_new.csv'

##########################################################################################################################################
### SUPPORTING FUNCTIONS
##########################################################################################################################################

default_args = {
    "owner":"airflow",
    'depends_on_past': False,
    "email_on_failure":False,
    "retries":1,
    "retry_delay":timedelta(minutes=1),
    'email_on_retry': False,
}




def add_data_to_vertica_table():

        """
        Reads data from a CSV file and inserts each row into the 'move.Radar' table in Vertica.
        Vertica connection details are provided via the 'vertica_conn_id'.
        """

        vertica = VerticaHook(vertica_conn_id = "vertica_conn_id",supports_autocommit= True)
          
        with open(PATH,"r") as f:
                file = csv.DictReader(f)
                rows = list(file)
        

        for row in rows:
                query = f"""INSERT INTO move.Radar(X,Y,ObjectID,siteID,location,radarType,antennaElevation,timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """
                values = (float(row['X']), float(row['Y']), int(row['OBJECTID']), row['siteID'],row['location'] ,row['radarType'],row['antennaElevation'],datetime.strptime(row['timestamp'], '%d-%m-%Y %H:%M'))
        
                vertica.run(sql = query,autocommit=True,parameters= values)



def vertica_status():
        """
        Checks the connectivity status of the Vertica database.

        Returns:
        - None if connection is successful and closed.
        - AirflowFailException if unable to connect.
        """

        try:
                vertica = VerticaHook(vertica_conn_id = "vertica_conn_id",supports_autocommit= True).get_conn()
                vertica.close()
                
        except Exception as e:
                return AirflowFailException(e)




##################################################################################################################################################
### MAIN DAG
##################################################################################################################################################

with DAG(dag_id = "csv_to_vertica",
         description="This pipeline will fetch radar_data.csv and store it into vertica table",
         catchup=False,start_date =datetime(2024,10,3),schedule_interval="@once",default_args=default_args ) as dag:
        

        ## Checks if radar_data.csv is available in the specified directory
        check_csv_exists = FileSensor(
                task_id = "check_csv_exists",
                filepath = PATH,
                poke_interval = 5,
                timeout = 60,
        )


        # check vertica is up and running
        check_vertica_status = PythonOperator(
                task_id =  "check_vertica_status",
                python_callable= vertica_status
        )


        # create table in vertica, establish connection to vertica using vertica conn id defined in airflow connections
        create_table =  VerticaOperator(
                task_id = "create_table",
                vertica_conn_id="vertica_conn_id",
                sql = """CREATE TABLE IF NOT EXISTS move.Radar (
                                X FLOAT,
                                Y FLOAT,
                                ObjectID INT,
                                siteID VARCHAR(255),
                                location VARCHAR(255),
                                radarType VARCHAR(255),
                                antennaElevation VARCHAR(255),
                                timestamp TIMESTAMP);"""
        )


        # This task is responsible for adding data to the vertica table
        add_csv_data = PythonOperator(
                task_id = "add_csv_data",
                python_callable= add_data_to_vertica_table
        )
        


        # Define the task execution order.
        check_csv_exists>> check_vertica_status >> create_table>> add_csv_data