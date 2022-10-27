from operator import index
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import (
    udf, col, lit, unix_timestamp, from_unixtime, date_format, concat, 
    monotonically_increasing_id, regexp_replace, regexp_extract
)
from pyspark.sql.types import FloatType, StringType
import datetime
import pg8000.native
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors


def get_lat_lon(val, coord):
    """
    This is a static method that just receives a string like
    POINT (lat, lon) and return the lat or lon coordinate.
    """    
    #print(val)
    val_lat_lon = str(val).replace("POINT (", "").replace(")", "").split(" ")
    if isinstance(val_lat_lon, list): 
        if len(val_lat_lon) < 2:
            print('!!!!!!!!!!!')
            print(val)
            print('!!!!!!!!!!!')
    if coord == "latitude":
        return float(val_lat_lon[1])
    elif coord == "longitude":
        return float(val_lat_lon[0])


def get_time_of_day(dt):
    """
    The intervals considered here were meant to divide a day in four parts, which
    would be more appropriate to find similar groups. A more restricted approach
    could be used here according to the business needs, considering more intervals, 
    for example.
    """
    time = datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S").time()
    if time >= datetime.time(0,0,0) and time < datetime.time(6,0,0):
        return "wee_hours"
    if time >= datetime.time(6,0,0) and time < datetime.time(12,0,0):
        return "morning"
    if time >= datetime.time(12,0,0) and time < datetime.time(18,0,0):
        return "afternoon"
    if time >= datetime.time(18,0,0) and time <= datetime.time(23,59,59):
        return "night"
    return ""


def get_week(dt):
    """
    This is a static method that just receives a string that represents
    a datetime and returns its week.
    """
    return datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S").strftime("%Y-%V")


spark = SparkSession.builder \
    .config("spark.jars", "/Users/ernando/drivers/postgresql-42.5.0.jar") \
    .appName('example_spark') \
    .getOrCreate()

print("Reading CSV...")

df = spark.read.csv("source/trips_100M.csv", header=True, encoding="UTF-8")
df = df.withColumn("id", monotonically_increasing_id())

print("Transforming lat/lon...")

udf_get_lat_lon = udf(get_lat_lon, FloatType())
#df = df.withColumn("orig_lat", udf_get_lat_lon(col("origin_coord"), lit("latitude")))
#df = df.withColumn("orig_lon", udf_get_lat_lon(col("origin_coord"), lit("longitude")))
#replace("POINT (", "").replace(")", "").split(" ")
reg_exp = "(-?[\d]*\.[\d]*)\s*(-?[\d]*\.[\d]*)"
df = df.withColumn("orig_lat", regexp_extract("origin_coord", reg_exp, 2).cast(FloatType()))
df = df.withColumn("orig_lon", regexp_extract("origin_coord", reg_exp, 1).cast(FloatType()))
df = df.withColumn("dest_lat", regexp_extract("destination_coord", reg_exp, 2).cast(FloatType()))
df = df.withColumn("dest_lon", regexp_extract("destination_coord", reg_exp, 1).cast(FloatType()))

df.show()

#df = df.drop("origin_coord")
#df = df.drop("destination_coord")

print("Transforming datetime...")

udf_get_time_of_day = udf(get_time_of_day, StringType())
df = df.withColumn("datetime", from_unixtime(unix_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss")))
df = df.withColumn("date", date_format(col("datetime"), "yyyy-MM-dd"))
df = df.withColumn("time", date_format(col("datetime"), "HH:mm:ss"))
df = df.withColumn("time_day", udf_get_time_of_day(col("datetime")))

print("Transforming datetime (week)...")

udf_get_week = udf(get_week, StringType())
df = df.withColumn("week", udf_get_week(col("datetime")))

df = df.drop("datetime")

print("Transforming data before loading it in database (orig lat/lon groups)...")

va = VectorAssembler(inputCols=["orig_lat", "orig_lon"], outputCol="features")
df = va.transform(df)
kmeans = KMeans(k=8, seed=1)
model = kmeans.fit(df)
df = model.transform(df).withColumnRenamed("prediction", "orig_cluster")
df = df.drop(f"features")

print("Transforming data before loading it in database (dest lat/lon groups)...")

va = VectorAssembler(inputCols=["dest_lat", "dest_lon"], outputCol="features")
df = va.transform(df)
kmeans = KMeans(k=8, seed=1)
model = kmeans.fit(df.select("features"))
df = model.transform(df)
df = df.withColumnRenamed("prediction", "dest_cluster")
df = df.drop(f"features")

print("Concatenating columns...")

#df = df.withColumn("orig_cluster", lit(1))
df = df.withColumn("dest_cluster", lit(1))

df = df.withColumn(
        "trip_group", 
        concat(
            lit("orig_"),
            col("orig_cluster"),
            lit("-dest_"),
            col("dest_cluster"),
            lit("-"),
            col("time_day")
        )
    )

print("Writing dataframe to the database...")

tablename = "trips"
jdbc_url = "jdbc:postgresql://localhost:5432/ernando"
df.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", jdbc_url) \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", tablename) \
    .option("user", "ernando") \
    .save()

con = pg8000.native.Connection(user="ernando", host="localhost", database="ernando")
con.run("CREATE INDEX ix_trips_region ON trips(region);")
con.run("CREATE INDEX ix_trips_coord ON trips(orig_lat, orig_lon, dest_lat, dest_lon);")
con.close()

print("done!")




