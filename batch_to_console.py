from pyspark.sql import SparkSession
import os

controller = os.getenv("PRAVEGA_CONTROLLER_URI", "tcp://127.0.0.1:9090")
scope = os.getenv("PRAVEGA_SCOPE", "examples")
allowCreateScope = os.getenv("PROJECT_NAME") is None

spark = (SparkSession
         .builder
         .getOrCreate()
         )

df = (spark
    .read
    .format("pravega") 
    .option("allow_create_scope", allowCreateScope)
    .option("controller", controller)
    .option("scope", scope) 
    .option("stream", "batchstream1")
    .load()
    .selectExpr("cast(event as string)", "scope", "stream", "segment_id", "offset")
 )

event_count = df.count()

df.show(1000, truncate=False)

print("Number of events in Pravega stream: %s" % event_count)
print("Done")
