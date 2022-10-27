import pandas as pd
import datetime
import numpy as np
#from sklearn.cluster import DBSCAN
from threading import Thread
import pg8000.native
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    udf, col, lit, unix_timestamp, from_unixtime, date_format, 
    concat, regexp_extract
)
from pyspark.sql.types import FloatType, StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans


class DatabaseHandler(Thread):
    """
    DatabaseHandler was created as an extension of Thread, allowing it to run
    the ingestion process in a separate thread. This way, the REST API does not 
    need to wait until the ending of the process to give a response to the client.
    This also allows the server to update the loading status asynchronously, which
    is used in the web socket connection to update the client.

    This class also has functions to transform, group and store the data. All
    the connections and database changes using PostgreSQL are made here.

    Finally, the functions related to calculate the weekly average number of trips
    by region or bounding box are present in this class, but they are not expected
    to run in a separate thread.  
    """

    def __init__(self, tablename, config={}):
        self._tablename = tablename
        self._csv_filename = config["csv_filename"]
        self._db_user = config["db_user"]
        self._db_password = config["db_password"]
        self._db_host = config["db_host"]
        self._db_port = config["db_port"]
        self._db_database = config["db_database"]
        self._jdbc_url = f"jdbc:postgresql://{self._db_host}:{self._db_port}/{self._db_database}"
        self._driver = config["db_driver"]
        self._driver_path = config["db_driver_path"]
        self._con = None
        self._df = pd.DataFrame()
        self.loading_status = []
        super().__init__()


    def run(self):
        """
        This method overrides the run method of Thread, which allows it
        to run "start()" is triggered.
        """
        self.load_csv()


    def load_csv(self):
        """
        This method contains the workflow of the loading process.
        It loads the CSV file in memory, then call the functions that 
        transforms the data to make it easier to find clusters and query
        data. With all transformed, the data is inserted in SQL database. 
        """
        self.loading_status.append("Reading source CSV...")

        spark = SparkSession.builder \
            .config("spark.jars", self._driver_path) \
            .appName('spark4trips') \
            .getOrCreate()
        self._df = spark.read.csv(self._csv_filename, header=True)
        self.loading_status.append("CSV file was loaded in memory.")
        
        self.loading_status.append("Transforming data before loading it in database (lat/lon columns)...")
        self._transform_lat_lon()

        self.loading_status.append("Transforming data before loading it in database (datetime columns)...")
        self._transform_date_time()

        self.loading_status.append("Transforming data before loading it in database (orig lat/lon groups)...")
        self._group_coordinates("orig")

        self.loading_status.append("Transforming data before loading it in database (dest lat/lon groups)...")
        self._group_coordinates("dest")

        self.loading_status.append("Transforming data before loading it in database (lat/lon/time groups)...")
        self._concat_orig_dest_timeday()

        self.loading_status.append("Data transformation has just finished.")

        self.loading_status.append("Loading data into the database...")
        self._prepare_database()
        self._df.write.format("jdbc") \
            .mode("overwrite") \
            .option("url", self._jdbc_url) \
            .option("driver", self._driver) \
            .option("dbtable", self._tablename) \
            .option("user", self._db_user) \
            .option("password", self._db_password) \
            .save()
        self.loading_status.append("Data has been loaded! Now indexing database for better performance...")
        self.create_db_indexes()
        final_msg = "Done! The loading process is now complete!"
        self.loading_status.append(final_msg)
        print(final_msg)


    def _prepare_database(self):
        """
        This method garantees that a new table will be created when loading
        the dataframe into the SQL database.
        """
        self._con = pg8000.native.Connection(
            user=self._db_user,
            password=self._db_password, 
            host=self._db_host, 
            port=self._db_port,
            database=self._db_database
        )
        self._con.run(f"DROP TABLE IF EXISTS {self._tablename};")
        self._con.close()


    def _transform_lat_lon(self):
        """
        Split lat and lon origin/destination, which will make it easier to find 
        similar points and also make some queries, specially when filtering bounding 
        box areas.
        """
        reg_exp = "(-?[\d]*\.[\d]*)\s*(-?[\d]*\.[\d]*)"
        self._df = self._df.withColumn("orig_lat", regexp_extract("origin_coord", reg_exp, 2).cast(FloatType()))
        self._df = self._df.withColumn("orig_lon", regexp_extract("origin_coord", reg_exp, 1).cast(FloatType()))
        self._df = self._df.withColumn("dest_lat", regexp_extract("destination_coord", reg_exp, 2).cast(FloatType()))
        self._df = self._df.withColumn("dest_lon", regexp_extract("destination_coord", reg_exp, 1).cast(FloatType()))
        self._df = self._df.drop("origin_coord")
        self._df = self._df.drop("destination_coord")


    def _transform_date_time(self):
        """
        Splits datetime in date, time, time day and week columns,
        which will be necessary to make some queries and find similar
        origin/destination/time of day.
        """
        udf_get_time_of_day = udf(self.get_time_of_day, StringType())
        self._df = self._df.withColumn("datetime", from_unixtime(unix_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss")))
        self._df = self._df.withColumn("date", date_format(col("datetime"), "yyyy-MM-dd"))
        self._df = self._df.withColumn("time", date_format(col("datetime"), "HH:mm:ss"))
        self._df = self._df.withColumn("time_day", udf_get_time_of_day(col("datetime")))
        udf_get_week = udf(self.get_week, StringType())
        self._df = self._df.withColumn("week", udf_get_week(col("datetime")))
        self._df = self._df.drop("datetime")


    @staticmethod
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


    @staticmethod
    def get_week(dt):
        """
        This is a static method that just receives a string that represents
        a datetime and returns its week.
        """
        return datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S").strftime("%Y-%V")


    def _group_coordinates(self, prefix):
        """
        This approach was chosen because it was the most scalable between the clustering
        alternatives in Apache Spark. My favorite option would be DBSCAN, which does not require
        the number of desired clusters in advance, but there is not a implementation for it in 
        pyspark.ml and the alternative implementations using Pandas were just not scalable according 
        to my local tests with 100 million rows. 
        The number of clusters set here was 8, but it would be necessary to test other params to check 
        which configuration would give the best combination of clusters.
        """
        va = VectorAssembler(inputCols=[f"{prefix}_lat", f"{prefix}_lon"], outputCol="features")
        self._df = va.transform(self._df)
        kmeans = KMeans(k=8, seed=1)
        model = kmeans.fit(self._df.select("features"))
        self._df = model.transform(self._df)
        self._df = self._df.withColumnRenamed("prediction", f"{prefix}_cluster")
        self._df = self._df.drop(f"features")


    def _concat_orig_dest_timeday(self):
        """
        This functions just concatenates the groups/clusters of origin/destination
        coordinates with the time of day column. This will allow to group trips
        that are similar in terms of origin, destination and time of day.
        """
        self._df = self._df.withColumn(
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


    def create_db_indexes(self):
        """
        After loading all data into database, creates some indexes that will
        improve the performance of the queries that will be executed to get
        the weekly average number of trips.
        """
        self._con = pg8000.native.Connection(
            user=self._db_user, 
            password=self._db_password,             
            host=self._db_host, 
            port=self._db_port,
            database=self._db_database
        )
        self._con.run("CREATE INDEX ix_trips_region ON trips(region);")
        self._con.run("CREATE INDEX ix_trips_coord ON trips(orig_lat, orig_lon, dest_lat, dest_lon);")
        self._con.close()


    def get_weekly_avg_qt_trips_by_region(self, region):
        """
        Queries the database to find the weekly average number of trips
        filtering a region.
        """
        query = """
            with reg_week as (
                SELECT  
                    t.region
                    , t.week
                    , count(1) as qt
                FROM
                    trips t
                WHERE 
                    UPPER(region) = '"""+region.upper()+"""'
                GROUP BY
                    t.region
                    , t.week 
            ) 
            SELECT 
                region,
                avg(qt) as avg_qt
            FROM
                reg_week
            GROUP BY
                region
            ;
        """
        self._con = pg8000.native.Connection(
            user=self._db_user, 
            password=self._db_password, 
            host=self._db_host, 
            port=self._db_port,
            database=self._db_database
        )
        result = self._con.run(query)
        return result[0][1] if result is not None else None


    def get_weekly_avg_qt_trips_by_bounding_box(self, coord):
        """
        Queries the database to find the weekly average number of trips
        filtering coordinates of a bounding box. First, these coordinates 
        need to be splitted, because lat/lon are together in the parameters
        received by the API.      
        """
        for bound in coord:
            bound_splitted = coord[bound].replace(",", " ").split(" ")
            coord[bound] = (bound_splitted[0], bound_splitted[-1])

        query = """
            with bb_week as (
                SELECT  
                    t.week
                    , count(1) as qt
                FROM
                    trips t
                WHERE 
                    t.orig_lat     <= """+coord["north-east"][0]+""" and t.orig_lon >= """+coord["north-east"][1]+""" 
                    and t.orig_lat >= """+coord["south-east"][0]+""" and t.orig_lon >= """+coord["south-east"][1]+"""
                    and t.orig_lat <= """+coord["north-west"][0]+""" and t.orig_lon <= """+coord["north-west"][1]+""" 
                    and t.orig_lat >= """+coord["south-west"][0]+""" and t.orig_lon <= """+coord["south-west"][1]+"""
                
                    and t.dest_lat <= """+coord["north-east"][0]+""" and t.dest_lon >= """+coord["north-east"][1]+""" 
                    and t.dest_lat >= """+coord["south-east"][0]+""" and t.dest_lon >= """+coord["south-east"][1]+"""
                    and t.dest_lat <= """+coord["north-west"][0]+""" and t.dest_lon <= """+coord["north-west"][1]+""" 
                    and t.dest_lat >= """+coord["south-west"][0]+""" and t.dest_lon <= """+coord["south-west"][1]+"""
                GROUP BY
                    t.week 
            )
            SELECT 
                avg(qt) as avg_qt
            FROM
                bb_week
            ;
        """
        self._con = pg8000.native.Connection(
            user=self._db_user, 
            password=self._db_password, 
            host=self._db_host, 
            port=self._db_port,
            database=self._db_database
        )
        result = self._con.run(query)
        return result[0][0] if result is not None else None

