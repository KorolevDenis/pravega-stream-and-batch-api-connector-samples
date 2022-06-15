from pyspark.sql import SparkSession
import os

controller = os.getenv("PRAVEGA_CONTROLLER_URI", "tcp://127.0.0.1:9090")
scope = os.getenv("PRAVEGA_SCOPE", "examples")
allowCreateScope = os.getenv("PROJECT_NAME") is None
checkPointLocation = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints-stream_generated_data_to_pravega")

spark = (SparkSession
         .builder
         .getOrCreate()
         )

(spark 
    .readStream 
    .format("rate") 
    .load() 
    .selectExpr("cast(timestamp as string) as event", "cast(value as string) as routing_key") 
    .writeStream 
    .trigger(processingTime="1 second") 
    .outputMode("append") 
    .format("pravega") 
    .option("allow_create_scope", allowCreateScope)
    .option("controller", controller)
    .option("scope", scope) 
    .option("stream", "streamprocessing1") 
    .option("checkpointLocation", checkPointLocation)
    .start() 
    .awaitTermination()
 )
