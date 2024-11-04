#####################################################################################################################################################
"""
This DAG acts as a consumer pipeline, where it consumes data from a specified Kafka topic. The consumed message is then passed to a subsequent task, 
which inserts the data into a Vertica table. This flow enables real-time data ingestion from Kafka to Vertica, ensuring that messages are consistently 
processed and stored within the Vertica database.
"""
##############################################################################################################################################

from airflow import DAG
from datetime import datetime,timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator 
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
from airflow.exceptions import AirflowFailException
from airflow.providers.vertica.hooks.vertica import VerticaHook
import json
from airflow.providers.vertica.operators.vertica import VerticaOperator




default_args = {
    "owner":"airflow",
    'depends_on_past': False,
    "email_on_failure":False,
    "retries":1,
    "retry_delay":timedelta(minutes=5),
    'email_on_retry': False,
}


############################################################################################################################################
### SUPPORTING FUNCTIONS
############################################################################################################################################



def consume_message(**context):

    """
    this function make this dag act as consumer , all the important conf are defined in kafka_conn_id like bootstrap.server , group.id etc
    """
    # Establish connection to Kafka consumer using specified Kafka connection ID(broker and other conf are defined inside connection) and topic
    consumer_hook = KafkaConsumerHook(
        topics=["MOVE-DATA"],
        kafka_config_id="kafka_conn_id",

    )
    consumer = consumer_hook.get_consumer()
    messages = consumer.consume(num_messages=1)

    if messages:
        for message in messages:
              # Decode the message from bytes to JSON format
            decoded_message = json.loads(message.value().decode('utf-8'))
            
            # Log the decoded message in task logs 
            print(decoded_message)
            
            # Push the decoded message to XCom to pass it to the next task
            context['ti'].xcom_push(key='kafka_message', value=decoded_message)
            
            
            consumer.commit()
            
            return decoded_message
    else:
         print("no message")


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



def add_data_to_vertica_table(**kwargs):
        """
        THIS function connect to vertica using vertica_conn_id, seek decoded message from consumer using xcom_pull
        and put the message into vertica table using insert command
        """
        vertica = VerticaHook(vertica_conn_id = "vertica_conn_id",supports_autocommit= True)
          
        row  = kwargs['ti'].xcom_pull(task_ids='consume-data',key = 'kafka_message')
        

        print(row)
        query = f"""INSERT INTO move.Radar(X,Y,ObjectID,siteID,location,radarType,antennaElevation,timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """
    
        row = json.loads(row)
        values = (row['X'], row['Y'], row['OBJECTID'], row['siteID'],row['location'] ,row['radarType'],row['antennaElevation'],datetime.strptime(row['timestamp'], '%d-%m-%Y %H:%M'))
        
        vertica.run(sql = query,autocommit=True,parameters= values)



##################################################################################################################################################
### MAIN DAG
##################################################################################################################################################


with DAG(dag_id="kafka_to_vertica",
         description="This Airflow DAG connects to a Kafka topic to consume messages and inserts the data into a Vertica table",
         schedule_interval="@once",
         default_args=default_args,
         start_date=datetime(2024,10,8),
         catchup=False) as dag:


    # Verifies the status of Ambari; ambari_conn_id should be set up in Airflow connections
    check_ambari_status = HttpSensor(
        task_id = "check_ambari_status",
        http_conn_id="ambari_conn_id",
        method="GET",
        response_check= lambda res : res.status_code in (200,201),
        endpoint="#/main/dashboard/metrics",
        poke_interval = 5,
        timeout = 300,)

    # tried this operator and it worked fine but could'nt able to send that data to other task
    # consume_data =  ConsumeFromTopicOperator(
    #     task_id = "consume_data" ,
    #     topics=['MOVE-DATA'],
    #     kafka_config_id="kafka_conn_id",
    #     max_messages=2,
    #     apply_function=process_message,
    #     # apply_function_kwargs={"ti": "{{ task_instance }}"}
    #     )
    

    # python operator which exec consume_message func and consume data from kafka broker
    consume_data = PythonOperator(
          task_id = "consume-data",
          python_callable=consume_message,
          provide_context = True
    )

    
    # check vertica status is up and running
    check_vertica_status = PythonOperator(
                task_id =  "check_vertica_status",
                python_callable= vertica_status
        )
    

    ## create table in vertica, in which consumed data will be stored
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
    

    # this task check for table exit or not in vertica database
    check_vertica_table = VerticaOperator(
                task_id = "check_table",
                vertica_conn_id="vertica_conn_id",
                sql = "select * from move.Radar limit 1")
    



    # This task is responsible for adding data to the vertica table
    add_kafka_message = PythonOperator(
                task_id = "add_kafka_message",
                python_callable= add_data_to_vertica_table,
                provide_context = True
        )



     # Define the task execution order.
    check_ambari_status  >> consume_data >> check_vertica_status>> create_table >> check_vertica_table >> add_kafka_message
