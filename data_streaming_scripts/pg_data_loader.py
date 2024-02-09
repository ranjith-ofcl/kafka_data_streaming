import psycopg2
import pandas as pd
import os

## PostgreSQL Connection
def psg_connection(dbname, user, password, host, port):
    try:
        conn = psycopg2.connect(
            dbname = dbname,
            user = user,
            password = password,
            host = host,
            port = port
        )
        return conn
    except psycopg2.Error as e:
        print("Error: Unable to connect with PostgreSQL database. Please check the connection.")
        print(e)
        return None

def load_genre_data(conn):
    try:
        cur = conn.cursor() 
        df = pd.read_csv('genre.csv') #Loading the data from "genre.csv"
        print(df)

        #Iterate each rows of values in dataframe df
        for index, value in df.iterrows():
            #PostgreSQL query to insert row values (genreid and genrename) into the table "genre"
            cur.execute("insert into genre (genreid, genrename) values (%s, %s)", (value['genreid'], value['genrename']))
            conn.commit() #Commiting the changes
            conn.close
        print("Data Loaded Successfully.")
    except Exception as e:
        print("Error: ", e)

def load_customer_data(conn):
    try:
        cur = conn.cursor() 
        df = pd.read_csv('Customer.csv') #Loading the data from "customer.csv"
        print(df)

        #Iterate each rows of values in dataframe df
        for index, value in df.iterrows():
            #PostgreSQL query to insert row values (genreid and genrename) into the table "genre"
            query = """
                        INSERT INTO public.customer(
                            customerid, firstname, lastname, company, address, city, state, country, postalcode, phone, fax, email
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """
            cur.execute(query, tuple(value))
            conn.commit() #Commiting the changes
            conn.close
        print("Data Loaded Successfully.")
    except Exception as e:
        print("Error: ", e)

def load_invoice_data(conn):
    try:
        cur = conn.cursor() 
        df = pd.read_csv('Invoice.csv') #Loading the data from "customer.csv"
        print(df)

        #Iterate each rows of values in dataframe df
        for index, value in df.iterrows():
            #PostgreSQL query to insert row values (genreid and genrename) into the table "genre"
            query = """
                        INSERT INTO public.invoice(
                            invoiceid, customerid, invoicedate, billingaddress, billingcity, billingstate, billingcountry, billingpostalcode, total
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """
            cur.execute(query, tuple(value))
            conn.commit() #Commiting the changes
            conn.close
        print("Data Loaded Successfully.")
    except Exception as e:
        print("Error: ", e)

dbname = os.environ.get("postgres_to_snowflake_dbname")
user = os.environ.get("postgres_user")
password = os.environ.get("postgres_user_pwd")
host = os.environ.get("postgres_host")
port = os.environ.get("postgres_port")

conn = psg_connection(dbname, user, password, host, port)
# load_genre_data(conn)
# load_customer_data(conn)
load_invoice_data(conn)



