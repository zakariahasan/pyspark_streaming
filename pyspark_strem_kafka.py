from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession\
            .builder\
            .appName('test100')\
            .master('local[3]')\
            .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.0')\
            .getOrCreate()
    
    stream_df = spark.readStream \
                .format("kafka") \
                .option("subscribe","test-123")\
                .option("kafka.bootstrap.servers","192.168.1.14:9092") \
                .option("startingOffsets","earliest")\
                .load()
    
    stream_df.printSchema()
    schema = StructType(
    [
       StructField(name = 'name', dataType=StringType(), nullable=True),
       StructField(name = 'address', dataType=StringType(), nullable=True),
    ]
    )
    
    value_df = stream_df.select(from_json(col("value").cast("string"),schema).alias("value"))
    value_df.printSchema()
    notification_df = value_df.select("value.name","value.address")
    #col_val.show(100)
    
    
    kafka_target_df = notification_df.selectExpr("name as key",
                                        """to_json(named_struct(
                                        'name',name,'address',address)) as  value """)
    
    notification_write_query = kafka_target_df\
        .writeStream\
        .queryName("Notification Writer")\
        .format("kafka")\
        .option("kafka.bootstrap.servers","192.168.1.14:9092")\
        .option("topic", "notifications")\
        .outputMode("append")\
        .option("checkpointLocation","chk-point-dir-2")\
        .start()
    
    
    notification_write_query.awaitTermination() 
    
