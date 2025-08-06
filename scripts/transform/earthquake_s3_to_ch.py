import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, split, trim, lower, md5, to_date, coalesce, date_format, current_timestamp, count
import argparse
import logging

parser = argparse.ArgumentParser()

parser.add_argument("--process-date", type=str, required=True)
parser.add_argument("--table-raw", type=str, required=True)
parser.add_argument("--table-agg", type=str, required=True)
args = parser.parse_args()

process_date = args.process_date
table_name_raw = args.table_raw
table_name_agg = args.table_agg


# ytd_date = subtract(days=1).to_date_string()
s3_path = f"s3a://prod/api/earthquake/events_{process_date}.json"
logging.basicConfig(level=logging.INFO)

spark = (
    SparkSession.builder.appName("test_s3_to_ch_session")
    .config("spark.ui.port", "4041")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

def load_transform(s3_path):
    df = spark.read.json(s3_path)
    if df.selectExpr("explode(features)") == []:
        logging.info("features in file are empty, uploading next date")
        return 

    cleared = (df.selectExpr("explode(features) as feature") \
        .select(
            col("feature.id").alias("id"),
            from_unixtime(col("feature.properties.time") / 1000).alias("ts"),
            col("ts").cast("date").alias("load_date"),
            #trim(split(col("feature.properties.place"), ",").getItem(0)).alias("place"),
            #trim(split(col("feature.properties.place"), ",").getItem(1)).alias("initial_region"),
            col("feature.properties.mag").alias("magnitude"),
            col("feature.properties.felt").alias("felt"),
            col("feature.properties.tsunami").alias("tsunami"),
            col("feature.properties.url").alias("url"),
            col("feature.geometry.coordinates")[0].alias("longitude"),
            col("feature.geometry.coordinates")[1].alias("latitude"),
            col("feature.geometry.coordinates")[2].alias("depth"),
            md5(lower(trim(col("feature.properties.place")))).alias("place_hash")
        )
        .withColumn("updated_at", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")))

    return cleared

def dump_ch(data, table_name):
    try:
        jdbc_url = 'jdbc:clickhouse://clickhouse:8123/default'
        db_user = os.getenv('CLICKHOUSE_USER')
        db_password = os.getenv('CLICKHOUSE_PASSWORD')
        table_name = table_name

        data.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("dbtable", table_name) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()
            
    except Exception as e:
        logging.info(f"Ошибка: {e}")    
    finally:
        logging.info(f"Cохранено: {data.count()} строк")    


def dump_agg(data, table_name):
    try:
        jdbc_url = 'jdbc:clickhouse://clickhouse:8123/default'
        db_user = os.getenv('CLICKHOUSE_USER')
        db_password = os.getenv('CLICKHOUSE_PASSWORD')
        table_name = table_name

        final_clear = data\
            .fillna({'place_hash':'other'})\
            .replace({'Russia Earthquake':'Russia'})\
            .where('''magnitude >5''')\
            .groupby('place_hash').agg(count('id').alias('count'))

        final_clear.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("dbtable", table_name) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()    

    except Exception as e:
        logging.info(f"Error: {e}")
    finally:
        logging.info(f"Cохранено: {data.count()} строк")   

df = load_transform(s3_path)
if df is not None:
    dump_ch(df, table_name_raw)
    dump_agg(df, table_name_agg)

spark.stop()

logging.info("Спарк сессия завершена")