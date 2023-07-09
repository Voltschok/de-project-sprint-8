import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql.functions import to_utc_timestamp, unix_timestamp, current_date, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql import functions as f
import psycopg2

 
# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka-admin\" password=\"de-kafka-admin-2022\";',
}

#колонки для выходного сообщения в kafka 
columns=['restaurant_id','adv_campaign_id',
 'adv_campaign_content', 'adv_campaign_owner',
 'adv_campaign_datetime_start' , 'adv_campaign_datetime_end', 
  'datetime_created', 'trigger_datetime_created' ]

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров

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
            .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
            .options(**kafka_security_options)\
            .option('topic', 'a_wolkov_out') \
            .save()
    # очищаем память от df
    df.unpersist()
 

# читаем из топика Kafka сообщения с акциями от ресторанов 
restaurant_read_stream_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
    .options(**kafka_security_options)\
    .option('subscribe', 'a_wolkov_in') \
    .load()

# определяем схему входного сообщения для json
incomming_message_schema = StructType( [
    StructField('restaurant_id', StringType(), True), 
    StructField('adv_campaign_id', StringType(), True),
    StructField('adv_campaign_content', StringType(), True),
    StructField('adv_campaign_owner', StringType(), True),
    StructField('adv_campaign_datetime_start', LongType(), True),
    StructField('adv_campaign_datetime_end', LongType(), True),
    StructField('datetime_created', LongType(), True)]) 

# определяем текущее время в UTC в миллисекундах
current_timestamp_utc = int(round(datetime.utcnow().timestamp()))
 
# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df =restaurant_read_stream_df.withColumn("value", restaurant_read_stream_df["value"].cast("string"))\
        .select( f.from_json("value", schema = incomming_message_schema).alias('value'))\
        .select('value.*')\
        .filter(f'adv_campaign_datetime_start <={current_timestamp_utc} and adv_campaign_datetime_end >={current_timestamp_utc}')
 

#вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = spark.read \
                    .format('jdbc') \
                    .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('dbtable', 'subscribers_restaurants') \
                    .option('user', 'student') \
                    .option('password', 'de-student') \
                    .load()
                    
# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = filtered_read_stream_df.join(subscribers_restaurant_df, 'restaurant_id', 'left')\
    .withColumn("trigger_datetime_created", f.lit(datetime.utcnow()).cast(TimestampType()))\
    .dropDuplicates(['restaurant_id', 'client_id']) \
    .withWatermark('trigger_datetime_created', '5 minute')

#создаем таблицу subscribers_feedback
connect_to_postgresql = psycopg2.connect(f"host='localhost' port='5432' dbname='de' user='jovyan' password='jovyan'")
cursor = connect_to_postgresql.cursor()
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

# запускаем стриминг    
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination() 
