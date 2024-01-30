from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import logging
from datetime import datetime


def create_keyspace(session):
   session.exec("""
       CREATE KEYSPACE IF NOT EXISTS spark_streams
       WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
   """)


   print("Keyspace successfully created")


def create_table(session):
   session.execute("""
   CREATE TABLE IF NOT EXISTS spark_streams.created_users (
       id UUID PRIMARY KEY,
       first_name TEXT,
       last_name TEXT,
       gender TEXT,
       address TEXT,
       post_code TEXT,
       email TEXT,
       username TEXT,
       registered_date TEXT,
       phone TEXT,
       picture TEXT);
   """)


   print("Table created successfully!")


def insert_data(session, **kwargs):
   print("inserting data...")


   id = kwargs.get("id")
   first_name = kwargs.get("first_name")
   last_name = kwargs.get("last_name")
   gender = kwargs.get("gender")
   address = kwargs.get("address")
   postcode = kwargs.get("postcode")
   email = kwargs.get("email")
   username = kwargs.get("username")
   dob = kwargs.get("dob")
   registered_date = kwargs.get("registered_date")
   phone = kwargs.get("phone")
   picture = kwargs.get("picture")


   try:
       session.execute("""
           INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address,
               post_code, email, username, dob, registered_date, phone, picture)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
       """, (id, first_name, last_name, gender, address,
             postcode, email, username, dob, registered_date, phone, picture))
       logging.info(f"Data inserted for the user {first_name} {last_name}")


   except Exception as e:
       logging.error(f'could not insert data due to {e}')




def create_spark_connection():
   s_conn = None


   try:
       s_conn = SparkSession.builder \
           .appName("SparkDataStreaming") \
           .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                          "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
           .config('spark.cassandra.connection.host', 'localhost') \
           .getOrCreate()
           # .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1", "org.apache.spark:spark-sql-kafka-0-10_2_12:3.4.1") \
           # .config("spark.cassandra.connection.host", "localhost") \
          
       s_conn.sparkContext.setLogLevel("ERROR")
       logging.info("spark connetion successfully created")
   except Exception as e:
       logging.error(f"could not create the spark sesssion due to exeption {e}")


   return s_conn       


def connect_to_kafka(spark_conn):
   spark_df = None
   try:
       spark_df = spark_conn.readStream \
           .format('kafka') \
           .option('kafka.bootstrap.servers', 'localhost:9092') \
           .option('subscribe', 'users_created') \
           .option('startingOffsets', 'earliest') \
           .load()
       logging.info("kafka dataframe created successfully")
   except Exception as e:
       logging.warning(f"kafka dataframe could not be created because: {e}")


   return spark_df  




def create_cassandra_connection():
  
   try:
       cluster = Cluster(["localhost"])
       cassandra_session = cluster.connect()


       return cassandra_session


   except Exception as e:
       logging.error(f" Could not create cassandra connetion due to {e}")
       return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
       StructField("id", StringType(), False),
       StructField("first_name", StringType(), False),
       StructField("last_name", StringType(), False),
       StructField("gender", StringType(), False),
       StructField("address", StringType(), False),
       StructField("post_code", StringType(), False),
       StructField("email", StringType(), False),
       StructField("username", StringType(), False),
       StructField("registered_date", StringType(), False),
       StructField("phone", StringType(), False),
       StructField("picture", StringType(), False)
   ])
   
    sel = spark_df.selectExpr("CAST(value AS STRING)")\
       .select(from_json(col("value"), schema).alias("data")).select("data.*")
    print(sel)


    return sel


if __name__ == "__main__":
   #creating the spark connetion
   spark_conn = create_spark_connection()


   if spark_conn is not None:
       # connecting to kafka with the spark connetion


       spark_df = connect_to_kafka(spark_conn)
       if spark_df is not None:
           selection_df = create_selection_df_from_kafka(spark_df)
           session = create_cassandra_connection()
           if session is not None:
               create_keyspace(session)
               create_table(session)
               #insert_data(session)
               streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                              .option("checkpointLocation", "/tmp/checkpoint")
                              .option("keyspace", "spark_streams")
                              .option("table", "created_users")
                              .start())
               streaming_query.awaitTermination()
       else:
           logging.error("kafka dataframe is None, check kafka connexion")   
 
        