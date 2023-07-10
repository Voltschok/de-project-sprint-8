import os
import configparser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql.functions import to_utc_timestamp, unix_timestamp, current_date, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType
from pyspark.sql import functions as f
import psycopg2



# Чтение параметров подключения для Kafka и Postgres из конфигурации
config = configparser.ConfigParser()

config.read('config.ini')

kafka_bootstrap_servers = config.get('Kafka', 'bootstrap_servers')
kafka_security_protocol = config.get('Kafka', 'security_protocol')
kafka_sasl_mechanism = config.get('Kafka', 'sasl_mechanism')
kafka_sasl_jaas_config = config.get('Kafka', 'sasl_jaas_config')


postgres_host=config.get('Postgres', 'host')
postgres_port=config.get('Postgres', 'port')
postgres_dbname=config.get('Postgres', 'dbname')
postgres_user=config.get('Postgres', 'user')
postgres_password=config.get('Postgres', 'password')

# Параметры безопасности Kafka
kafka_security_options = {
    'kafka.security.protocol': kafka_security_protocol,
    'kafka.sasl.mechanism': kafka_sasl_mechanism,
    'kafka.sasl.jaas.config': kafka_sasl_jaas_config,
}

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )
 

#колонки для выходного сообщения в kafka 
columns=['restaurant_id','adv_campaign_id',
 'adv_campaign_content', 'adv_campaign_owner',
 'adv_campaign_datetime_start' , 'adv_campaign_datetime_end', 
  'datetime_created', 'trigger_datetime_created' ]


# Модуль 0: Создание таблицы subscribers_feedback
def create_subscribers_feedback_table():

    try:
        connect_to_postgresql = psycopg2.connect(f"host={postgres_host} port={postgres_port} dbname={postgres_dbname} user={postgres_user} password={postgres_password}")
        cursor = connect_to_postgresql.cursor()
  
    # Здесь выполняется запись данных в PostgreSQL
        cursor.execute(""" 
        CREATE TABLE IF NOT EXISTS public.subscribers_feedback ( 
        id serial4 NOT NULL, 
        restaurant_id text NOT NULL, 
        adv_campaign_id text NOT NULL, 
        adv_campaign_content text NOT NULL, 
        adv_campaign_owner text NOT NULL, 
        adv_campaign_owner_contact text NOT NULL, 
        adv_campaign_datetime_start int8 NOT NULL, 
        adv_campaign_datetime_end int8 NOT NULL, 
        datetime_created int8 NOT NULL, 
        client_id text NOT NULL, 
        trigger_datetime_created int4 NOT NULL, 
        feedback varchar NULL, 
        CONSTRAINT id_pk PRIMARY KEY (id) 
    ); 
    """) 
        connect_to_postgresql.commit() 
        # cursor.close()
        # connect_to_postgresql.close()
    except psycopg2.Error as e:
        # Обработка ошибки подключения к PostgreSQL
        print("Ошибка при подключении к PostgreSQL:", e) 

# Модуль 1: Инициализация Spark сессии
def initialize_spark_session():
    spark = SparkSession.builder \
        .appName("RestaurantSubscribeStreamingService") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()
    return spark


# Модуль 2: Чтение данных из Kafka
def read_from_kafka(spark):
    restaurant_read_stream_df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', kafka_bootstrap_servers) \
        .options(**kafka_security_options)\
        .option('subscribe', 'a_wolkov_in') \
        .load()
 
    return restaurant_read_stream_df

# Модуль 3: Добавление схемы входных данных
def add_message_schema(df):
    incomming_message_schema = StructType([
        StructField('restaurant_id', StringType(), True),
        StructField('adv_campaign_id', StringType(), True),
        StructField('adv_campaign_content', StringType(), True),
        StructField('adv_campaign_owner', StringType(), True),
        StructField('adv_campaign_datetime_start', LongType(), True),
        StructField('adv_campaign_datetime_end', LongType(), True),
        StructField('datetime_created', LongType(), True)])
    
    global current_timestamp_utc
    current_timestamp_utc = int(round(datetime.utcnow().timestamp())) 

    filtered_read_stream_df = df.withColumn("value", df["value"].cast("string"))\
        .select(f.from_json("value", schema=incomming_message_schema).alias('value'))\
        .select('value.*')\
        .filter(f'adv_campaign_datetime_start <={current_timestamp_utc} and adv_campaign_datetime_end >={current_timestamp_utc}')
    
    return filtered_read_stream_df

# Модуль 4: Чтение данных из PostgreSQL
def read_from_postgresql(spark):
    subscribers_restaurant_df = spark.read \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', 'subscribers_restaurants') \
        .option('user', 'student') \
        .option('password', 'de-student') \
        .load()
 
    return subscribers_restaurant_df

#Модуль 5: джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
def join_data(filtered_read_stream_df, subscribers_restaurant_df):
    
    result_df = filtered_read_stream_df.join(subscribers_restaurant_df, 'restaurant_id', 'left')\
        .withColumn("trigger_datetime_created", f.lit(datetime.utcnow()).cast(TimestampType()))\
        .dropDuplicates(['restaurant_id', 'client_id'])\
        .withWatermark('trigger_datetime_created', '5 minute') 
    result_df.printSchema()
    return result_df

# Модуль 6: Отправка сообщений в Postgres (с колонкой feedback) и в Kafka (без колонки feedback)
def foreach_batch_function(df, epoch_id):

    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()

    # записываем df в PostgreSQL с полем feedback 
    df.withColumn('feedback', f.lit(None).cast(StringType())).write.format('jdbc')\
                    .mode('append')\
                    .option('url', 'jdbc:postgresql://localhost:5432/de') \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('dbtable', 'public.subscribers_feedback') \
                    .option('user', 'jovyan') \
                    .option('password', 'jovyan')\
                    .save()  
  
    # создаём df для отправки в Kafka. Сериализация в json.
        # отправляем сообщения в результирующий топик Kafka без поля feedback
    df.withColumn('value', f.to_json(f.struct(columns))).select('value')\
            .write\
            .format("kafka")\
            .option('kafka.bootstrap.servers', kafka_bootstrap_servers) \
            .options(**kafka_security_options)\
            .option('topic', 'a_wolkov_out') \
            .save() 
    # очищаем память от df 
    df.unpersist()

# Модуль 7: Запуск стриминга
def main():
    spark=initialize_spark_session()
    incoming_message=read_from_kafka(spark)
    filtered_read_stream_df=add_message_schema(incoming_message)
    subscribers_restaurant_df=read_from_postgresql(spark)
        
    result_df=join_data(filtered_read_stream_df, subscribers_restaurant_df)
    result_df.writeStream \
            .foreachBatch(foreach_batch_function) \
            .start() \
            .awaitTermination() 


if __name__ == "__main__":
        create_subscribers_feedback_table()
        main()
