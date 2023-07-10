import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql.functions import to_utc_timestamp, unix_timestamp, current_date, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql import functions as f
import psycopg2



# Чтение параметров подключения из конфигурации
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
config.read(config_path)

kafka_bootstrap_servers = config.get('Kafka', 'bootstrap_servers')
kafka_security_protocol = config.get('Kafka', 'security_protocol')
kafka_sasl_mechanism = config.get('Kafka', 'sasl_mechanism')
kafka_sasl_jaas_config = config.get('Kafka', 'sasl_jaas_config')


postgres_host=config.get('Postgres', 'host')
postgres_port=config.get('Postgres', 'port')
postgres_dbname=config.get('Postgres', 'dbname')
postgres_user=config.get('Postgres', 'user')
postgres_password=config.get('Postgres', 'password')

# host='localhost'
# port='5432'
# dbname='de'
# user='jovyan'
# password='jovyan'"


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
# kafka_security_options = {
#     'kafka.security.protocol': 'SASL_SSL',
#     'kafka.sasl.mechanism': 'SCRAM-SHA-512',
#     'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka-admin\" password=\"de-kafka-admin-2022\";',
# }

#колонки для выходного сообщения в kafka 
columns=['restaurant_id','adv_campaign_id',
 'adv_campaign_content', 'adv_campaign_owner',
 'adv_campaign_datetime_start' , 'adv_campaign_datetime_end', 
  'datetime_created', 'trigger_datetime_created' ]







# Модуль 1: Инициализация Spark сессии
def initialize_spark_session():
    spark_jars_packages = ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0",
    ])

    spark = SparkSession.builder \
        .appName("RestaurantSubscribeStreamingService") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()
    return spark

# Модуль 2: Запись данных в PostgreSQL
def write_to_postgresql(df):
    df.withColumn('feedback', f.lit(None).cast(StringType())).write.format('jdbc')\
        .mode('append')\
        .option('url', 'jdbc:postgresql://localhost:5432/de') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', 'public.subscribers_feedback') \
        .option('user', 'jovyan') \
        .option('password', 'jovyan')\
        .save()

# Модуль 3: Отправка сообщений в Kafka
def send_to_kafka(df):
    df.withColumn('value', f.to_json(f.struct(columns))).select('value')\
        .write\
        .format("kafka")\
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .options(**kafka_security_options)\
        .option('topic', 'a_wolkov_out') \
        .save()

# Модуль 4: Чтение данных из Kafka
def read_from_kafka():
    restaurant_read_stream_df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .options(**kafka_security_options)\
        .option('subscribe', 'a_wolkov_in') \
        .load()
    return restaurant_read_stream_df

# Модуль 5: Добавление схемы входных данных
def add_message_schema(df):
    incomming_message_schema = StructType([
        StructField('restaurant_id', StringType(), True),
        StructField('adv_campaign_id', StringType(), True),
        StructField('adv_campaign_content', StringType(), True),
        StructField('adv_campaign_owner', StringType(), True),
        StructField('adv_campaign_datetime_start', LongType(), True),
        StructField('adv_campaign_datetime_end', LongType(), True),
        StructField('datetime_created', LongType(), True)])
    
    current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

    filtered_read_stream_df = df.withColumn("value", df["value"].cast("string"))\
        .select(f.from_json("value", schema=incomming_message_schema).alias('value'))\
        .select('value.*')\
        .filter(f'adv_campaign_datetime_start <={current_timestamp_utc} and adv_campaign_datetime_end >={current_timestamp_utc}')
    
    return filtered_read_stream_df

# Модуль 6: Чтение данных из PostgreSQL
def read_from_postgresql():
    subscribers_restaurant_df = spark.read \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', 'subscribers_restaurants') \
        .option('user', 'student') \
        .option('password', 'de-student') \
        .load()
    
    return subscribers_restaurant_df

# Модуль 7: Создание таблицы subscribers_feedback
def create_subscribers_feedback_table():
    connect_to_postgresql = psycopg2.connect(f"host={postgres_host} port={postgres_port} dbname={postgres_db} user={postgres_user} password={postgres_password}")
    cursor = connect_to_postgresql.cursor()
    
    try:
        connection = psycopg2.connect(host='localhost', port='5432', dbname='de', user='jovyan', password='jovyan')
        cursor = connection.cursor()

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
    cursor.close()
    connection.close()
    except psycopg2.Error as e:
        # Обработка ошибки подключения к PostgreSQL
        print("Ошибка при подключении к PostgreSQL:", e) 

# Модуль 8: Запуск стриминга
def start_streaming(df):
    df.writeStream \
        .foreachBatch(foreach_batch_function) \
        .start() \
        .awaitTermination() 
