# mongodb-pyspark-integration
MongoDB and PySpark integration project demonstrating data ingestion, querying, and processing using MongoDB Atlas and Spark.

# MongoDB + PySpark Integration

## Overview
This project demonstrates:
- inserting JSON data into MongoDB
- querying nested fields in MongoDB
- reading MongoDB data into PySpark
- filtering nested array data using PySpark
- writing Spark DataFrames back to MongoDB

## MongoDB Insert
```javascript
use youtubeDB

db.youtube.insert({
  kind: "youtube#searchListResponse",
  etag: "\"m2yskBQFythfE4irbTIeOgYYfBU/PaiEDiVxOyCWelLPuuwa9LKz3Gk\"",
  nextPageToken: "CAUQAA",
  regionCode: "KE",
  pageInfo: {
    totalResults: 4249,
    resultsPerPage: 5
  },
  items: [
    {
      kind: "youtube#searchResult",
      etag: "\"m2yskBQFythfE4irbTIeOgYYfBU/QpOIr3QKlV5EUlzfFcVvDiJT0hw\"",
      id: {
        kind: "youtube#channel",
        channelId: "UCJowOS1R0FnhipXVqEnYU1A"
      }
    },
    {
      kind: "youtube#searchResult",
      etag: "\"m2yskBQFythfE4irbTIeOgYYfBU/AWutzVOt_5p1iLVifyBdfoSTf9E\"",
      id: {
        kind: "youtube#video",
        videoId: "Eqa2nAAhHN0"
      }
    },
    {
      kind: "youtube#searchResult",
      etag: "\"m2yskBQFythfE4irbTIeOgYYfBU/2dIR9BTfr7QphpBuY3hPU-h5u-4\"",
      id: {
        kind: "youtube#video",
        videoId: "IirngItQuVs"
      }
    }
  ]
})

## MongoDB Query
```javascript
db.youtube.find(
  { "items.id.channelId": "UCJowOS1R0FnhipXVqEnYU1A" },
  { _id: 0, regionCode: 1 }
)
```

## PySpark Read from MongoDB
```python
df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
.option("uri", "mongodb+srv://<username>:<password>@cluster/.../youtubeDB.youtube") \
.load()

df.show()
```

## PySpark Filter
```python
from pyspark.sql.functions import explode, col

df1 = df.withColumn("item", explode("items"))

df1.filter(
    col("item.id.channelId") == "UCJowOS1R0FnhipXVqEnYU1A")
.select("regionCode").show()
```

## PySpark Write to MongoDB
```python
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
```

## Note
Credentials are removed for security.
