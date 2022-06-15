from pyspark.sql import SparkSession
import os

controller = os.getenv("PRAVEGA_CONTROLLER_URI", "tcp://127.0.0.1:9090")
scope = os.getenv("PRAVEGA_SCOPE", "examples")
allowCreateScope = os.getenv("PROJECT_NAME") is None
filename = "sample_data.json"

spark = (SparkSession
    .builder 
    .getOrCreate()
)

df = (spark
    .read
    .format("json")
    .load(filename)
    .selectExpr(
        "to_json(struct(*)) as event", 
        "key as routing_key"            
    )
)

df.show(20, truncate=False)

(df
    .write
    .mode("append")
    .format("pravega")
    .option("allow_create_scope", allowCreateScope)
    .option("controller", controller)
    .option("scope", scope)
    .option("stream", "batchstream1")
    .option("default_num_segments", "5")
    .save()
)

print("Done")
