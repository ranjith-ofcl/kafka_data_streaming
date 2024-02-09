from kafka import KafkaConsumer
import pandas as pd
import json
import snowflake.connector as sfc
import os

#Creating Consumer
try:
    bootstrap_servers = ['localhost:29092']
    topic_name = 'source.public.genre'

    consumer = KafkaConsumer(
        topic_name, 
        auto_offset_reset = 'earliest', 
        bootstrap_servers = bootstrap_servers,
        group_id = 'my_consumer-grp-genre'  
        )
except Exception as e:
    print("Error: ", e)
#Snowflake connection
def sfc_connection(user, password, account, warehouse, database, schema):
    try:
        conn = sfc.connect(
            user = user,
            password = password,
            account = account,
            warehouse = warehouse,
            database = database,
            schema = schema
        )
        return conn #returning connection
    except sfc.Error as e:
        print("Error: Unable to connect with PostgreSQL database. Please check the connection.")
        print(e)
        return None


def ingestion(values, conn):
    cursor = conn.cursor()
    try:
        # Spitting the dictionary into values and assigning variable, so that we can use it as insert or update or delete value in snowflake query
        # Also creating variables for target location, such as database name, schema cname and table name.
        # database name and schema names are already pushed to envirnmental variable for security purposes. Actual values are present in readme.md file
        # defining del_or_not variable to identify the message is to delete the record in snowflake or to other transaction in snowflake
        genreid = values['genreid']
        genrename = values['genrename']
        del_or_not = values['__deleted']
        target_database = os.environ.get("sfc_database")
        target_schema = os.environ.get("sfc_schema")
        target_table = "GENRE"
        
        # in order to decide that we need to insert or update or delete in Snowlake table, weneed to fine the record is already present in that table or not, 
        # For that, querying the count(*) from genre table where genreid is received genreid from message
        cursor.execute(f'SELECT COUNT(*) FROM {target_database}.{target_schema}.{target_table} WHERE GENREID in ({genreid})')
        result = cursor.fetchone()[0]

        #Defining Logic to deciode the process Insert or Update or Delete
        # 1. If the record is already present in SNowflake, and if it's a delete transaction,THEN deleting the record from snowflake
        # 2. If the record is already present in Snowflake, and if it's not a delete transaction,THEN update the value (genrename)
        # 3. If the record is not present in snowflake, and if it's not a delete transaction, THEN insert the record to snowflake table

        if result != 0 and del_or_not == 'true':
            del_query = f'DELETE FROM {target_database}.{target_schema}.{target_table} WHERE GENREID IN ({genreid});'
            cursor.execute(del_query)
            print(f"Record {genreid} has been deleted successfully.")
        elif result != 0 and del_or_not == 'false':
            update_query = f"UPDATE {target_database}.{target_schema}.{target_table} SET genrename = '{genrename}' WHERE genreid = '{genreid}'"
            cursor.execute(update_query)
            print(f"Record {genreid} has been updated successfully as {genrename}")
        elif result == 0 and del_or_not == 'false':
            insert_query = f"INSERT INTO {target_database}.{target_schema}.{target_table} (genreid, genrename) VALUES ({genreid}, '{genrename}')"
            cursor.execute(insert_query)
            print(f"Record {genreid} has been inserted successfully with {genrename}")
        conn.commit()
    except Exception as e: 
        print(f"Error: {e}")

def initiate_kafka(conn):
    print("\n!!!KAFKA STARTED TO LISTEN FOR STREAMING!!!\n")
    # Gettings the messages from consumer and getting the value content of the message.
    # Once we got the values in bytes format, decoding the bytes to string. 
    # Once it's done, loading the string to dictionary and passing the value to ingestion function to ingest into snowflake
    for message in consumer:
        # print("Whole message is: ", message)
        str_value = message.value.decode('utf-8')
        msg = json.loads(str_value)
        ingestion(msg, conn)

#Defining Environmental Variables
user = os.environ.get("sfc_user")
password = os.environ.get("sfc_pwd")
account = os.environ.get("sfc_account")
warehouse = os.environ.get("sfc_warehouse")
database = os.environ.get("sfc_database")
schema = os.environ.get("sfc_schema")

conn = sfc_connection(user, password, account, warehouse, database, schema) #received Snowflake connection
initiate_kafka(conn) 

    
    


