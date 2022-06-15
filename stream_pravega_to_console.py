from pyspark.sql import SparkSession
import os

controller = os.getenv("PRAVEGA_CONTROLLER_URI", "tcp://127.0.0.1:9090")
scope = os.getenv("PRAVEGA_SCOPE", "examples")
allowCreateScope = os.getenv("PROJECT_NAME") is None
checkPointLocation = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints-stream_pravega_to_console")

spark = (SparkSession
         .builder
         .getOrCreate()
         )

(spark 
    .readStream 
    .format("pravega") 
    .option("allow_create_scope", allowCreateScope)
    .option("controller", controller)
    .option("scope", scope) 
    .option("stream", "streamprocessing1")
    # If there is no checkpoint, start at the earliest event.
    .option("start_stream_cut", "latest")
    .load() 
    .selectExpr("cast(event as string)", "scope", "stream", "segment_id", "offset")
    .writeStream 
    .trigger(processingTime="1 second") 
    .outputMode("append") 
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", checkPointLocation)
    .start() 
    .awaitTermination()
 )
