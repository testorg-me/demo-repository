module.exports.MONGO_CONNECTION_STRING =
    "mongodb://fbalbdjhejdxnsjdns-dev:jdjhwjkdmndmwnmdsna4@sp-dddedjkdjnjfop.mongoddb.net:27017,sp-dekjwkdnjmnep.mongodb.net:27017,sp-dfdsdstop.mongodb.net:27017/buddydb?ssl=true&replicaSs=true&w=majority";

module.exports.HTTP_PORT = process.env.PORT || 80110;

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from datetime import datetime, timedelta
from pyspark.sql.types import *
import os
import json
import boto3



UUID_SAMPLE_FRACTION = 0.01
SLACK_CHANNEL_NAME = "data-lake-heuristic-notification"


def daily_user_location_heuristic():
    spark = SparkSession \
        .builder \
        .appName("userLocation Heuristic") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.4.1') \
        .config('spark.sql.caseSensitive', 'true') \
        .getOrCreate()

    today = datetime.now().strftime("%Y-%m-%d")
    start_date = datetime.strptime(today, '%Y-%m-%d') + timedelta(days=-1)
    end_date = datetime.strptime(today, '%Y-%m-%d')
    print(start_date)
    """
    Get Sync data UUID
    """
    schema = StructType([StructField("uuid", StringType(), True),
                         StructField("updatedAt", TimestampType(), True)])

    data_sync_df = spark.read.format("mongo").option(
        "uri",
        "mongodb://blablablabblabla:blabalbksndfjhejdebhebedw@sdjnjdjwd-00-0ejdkf.mongodb.net:27017/bddsserfuddfeedydb.dataSynrcStatus?authSource=admin&replicaSet=sp-prod-rs-shard-0&readPreference=secondary&appname=data_lake_heuristic&ssl=true"
    ).load(schema=schema)
    data_sync_df.printSchema()
    data_sync_df = data_sync_df.where(
        (data_sync_df['updatedAt'] > start_date) & (data_sync_df['updatedAt'] < end_date))

    data_sync_df.show()
    uuid_list_df = data_sync_df.select('uuid').distinct()
    uuid_list_df = uuid_list_df.sample(withReplacement=False, fraction=UUID_SAMPLE_FRACTION)
    uuid_list = uuid_list_df.select("uuid").rdd.flatMap(lambda x: x).collect()
    uuid_list = uuid_list[:200]
    print("Output list")
    print(uuid_list)

    """
    Get data from required collection for uuid found
    """
    mongo_data_df = spark.read.format("mongo").option(
        "uri",
        "mongodb://blablablabblabla:blabalbksndfjhejdebhebedw@sdjnjdjwd-00-0ejdkf.mongodb.net:27017/bddsserfuddfeedydb.dataSynrcStatus?authSource=admin&replicaSet=sp-prod-rs-shard-0&readPreference=secondary&appname=data_lake_heuristic&ssl=true"
    ).load()
    mongo_data_df.printSchema()
    mongo_data_df = mongo_data_df.where(mongo_data_df['uuid'].isin(uuid_list))
    mongo_data_df = mongo_data_df.where(
        (mongo_data_df['timeTakenAt'] > start_date) & (mongo_data_df['timeTakenAt'] < end_date))
    cols = ['uuid', 'latitude', 'longitude']
    mongo_data_df = mongo_data_df.select(*cols)
    #mongo_data_df.show()

    """
