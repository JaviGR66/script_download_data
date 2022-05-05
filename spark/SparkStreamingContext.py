import findspark
import pyspark
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext

LOG_LEVEL = "ERROR"
BATCH_DURATION = 3
IP = "localhost"
PORT = 10007
# PORT = 55408


class SparkStreamingContext:
    def __init__(self):
        findspark.init()
        spark = SparkSession.builder.appName('SERVER_01').getOrCreate()
        sc = spark.sparkContext

        # Create a local StreamingContext with two working thread and batch interval of 1 second
        sc.setLogLevel(LOG_LEVEL)
        # ssc = StreamingContext(sc, 1)
        ssc = StreamingContext(sc, batchDuration=BATCH_DURATION)

        # We need to create the checkpoint
        ssc.checkpoint("checkpoint")

        # Create a DStream that will connect to hostname:port, like localhost:9999
        lines = ssc.socketTextStream(IP, PORT)

        #lines.filter(lambda tweet: tweet != "").map(lambda tweet: sentimentAnalyser(tweet)).foreachRDD(
        #    lambda tweet: saveToDB(tweet))

        lines.foreachRDD(lambda licitation: print(licitation))

        ssc.start()  # Start the computation
        ssc.awaitTermination()  # Wait for the computation to terminate
