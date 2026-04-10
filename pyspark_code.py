from pyspark.sql.functions import explode, col

df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
.option("uri", "mongodb+srv://<username>:<password>@cluster/.../youtubeDB.youtube") \
.load()

df.show()

df1 = df.withColumn("item", explode("items"))

df1.filter(
    col("item.id.channelId") == "UCJowOS1R0FnhipXVqEnYU1A"
).select("regionCode").show()

sales = spark.createDataFrame([
    (1, 100),
    (2, 200),
    (3, 300),
    (1, 100),
    (2, 200),
    (3, 300),
    (1, 100),
    (2, 200),
    (3, 300)
], ["ProductId", "Sales"])

sales.write.format("com.mongodb.spark.sql.DefaultSource") \
.mode("append") \
.option("uri", "mongodb+srv://<username>:<password>@cluster/.../youtubeDB.sales") \
.save()
