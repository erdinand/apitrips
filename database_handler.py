import pandas as pd
import sqlite3 as sl
import datetime
import numpy as np
from sklearn.cluster import DBSCAN
from threading import Thread
import time


class DatabaseHandler(Thread):
    """
    DatabaseHandler was created as an extension of Thread, allowing it to run
    the ingestion process in a separate thread. This way, the REST API does not 
    need to wait until the ending of the process to give a response to the client.
    This also allows the server to update the loading status asynchronously, which
    is used in the web socket connection to update the client.

    This class also has functions to make transform, group and store the data. All
    the connections and database changes using SQLite are made here.

    Finally, the functions related to calculate the weekly average number of trips
    by region or bounding box are present in this class, but they are not expected
    to run in a separate thread.  
    """

    __DB_FOLDER = "database"
    __SOURCE_FOLDER = "source"

    def __init__(self, tablename, csv_filename=''):
        self._tablename = tablename
        self._csv_filename = csv_filename
        self._database_path = f"{self.__DB_FOLDER}/{self._tablename}-db.db"
        self._con = sl.connect(self._database_path, check_same_thread=False)
        self._df = pd.DataFrame()
        self.loading_status = []
        super().__init__()


    def run(self):
        """
        This method overrides the run method of Thread, which allows it
        to run "start()" is triggered.
        """
        self.load_csv(self._csv_filename)


    def load_csv(self, csv_filename):
        """
        This method contains the workflow of the loading process.
        It loads the CSV file in memory, then call the functions that 
        transforms the data to make it easier to find clusters and query
        data. With all transformed, the data is inserted in SQL database. 
        """
        self.loading_status.append("Reading source CSV...")
        csv_source_file = f"{self.__SOURCE_FOLDER}/{csv_filename}"
        self._df = pd.read_csv(csv_source_file)
        self.loading_status.append("CSV file was loaded in memory.")
        
        self.loading_status.append("Transforming data before loading it in database...")
        self._transform_lat_lon()
        self._transform_date_time()
        self._group_coord()
        self._concat_orig_dest_timeday()
        self.loading_status.append("Data transformation has just finished.")

        self.loading_status.append("Loading data into the database...")
        self._prepare_database()
        self._df.to_sql(self._tablename, self._con)
        self.loading_status.append("Data has been loaded! Now indexing database for better performance...")
        self.create_db_indexes()
        self.loading_status.append("Done! The loading process is now complete!")
        print("Done! The trips database has been loaded.")


    def _prepare_database(self):
        """
        This method garantees that a new table will be created when loading
        the dataframe into the SQL database.
        """
        self._con.execute(f"DROP TABLE IF EXISTS {self._tablename};")


    def _transform_lat_lon(self):
        """
        Split lat and lon origin/destination, which will make it easier to find 
        similar points and also make some queries, specially when filtering bounding 
        box areas.
        """
        self._df['orig_lat'] = self._df["origin_coord"].apply(lambda row: self.get_lat_lon(row, "latitude"))
        self._df['orig_lon'] = self._df["origin_coord"].apply(lambda row: self.get_lat_lon(row, "longitude"))
        self._df['dest_lat'] = self._df["destination_coord"].apply(lambda row: self.get_lat_lon(row, "latitude"))
        self._df['dest_lon'] = self._df["destination_coord"].apply(lambda row: self.get_lat_lon(row, "longitude"))
        self._df.drop("origin_coord", axis=1, inplace=True)
        self._df.drop("destination_coord", axis=1, inplace=True)


    @staticmethod
    def get_lat_lon(val, coord):
        """
        This is a static method that just receives a string like
        POINT (lat, lon) and return the lat or lon coordinate.
        """
        val_lat_lon = str(val).replace("POINT (", "").replace(")", "").split(" ")
        if coord == "latitude":
            return float(val_lat_lon[1])
        elif coord == "longitude":
            return float(val_lat_lon[0])
        else:
            return ""


    def _transform_date_time(self):
        """
        Splits datetime in date, time, time day and week columns,
        which will be necessary to make some queries and find similar
        origin/destination/time of day.
        """
        self._df["datetime"] = pd.to_datetime(self._df["datetime"])
        self._df["date"] = self._df["datetime"].apply(lambda row: row.date())
        self._df["time"] = self._df["datetime"].apply(lambda row: row.time())
        self._df["time_day"] = self._df["time"].apply(lambda row: self.get_time_of_day(row))
        self._df["week"] = self._df["datetime"].apply(lambda row: row.strftime("%Y-%V"))
        self._df.drop("datetime", axis=1, inplace=True)


    @staticmethod
    def get_time_of_day(time):
        """
        The intervals considered here were meant to divide a day in four parts, which
        would be more appropriate to find similar groups. A more restricted approach
        could be used here according to the business needs, considering more intervals, 
        for example.
        """
        if time >= datetime.time(0,0,0) and time < datetime.time(6,0,0):
            return "wee_hours"
        if time >= datetime.time(6,0,0) and time < datetime.time(12,0,0):
            return "morning"
        if time >= datetime.time(12,0,0) and time < datetime.time(18,0,0):
            return "afternoon"
        if time >= datetime.time(18,0,0) and time <= datetime.time(23,59,59):
            return "night"
        return ""


    def _group_coord(self):
        """
        The approach used here was based mainly in the approach described in 
        https://geoffboeing.com/2014/08/clustering-to-reduce-spatial-data-set-size/ .
        Using DBSCAN seems the better way to find clusters based on lat/lon coordinates.
        Making some tests using the provided dataset, I found out that the minimum distance
        between the points to find at least one group when considering time of day was 
        1.6 kilometers. This is why I chose this parameter, but it could be increased/decreased
        according to the business needs. Transformations to radians, algorithm and metric params
        were kept as they seem the best for finding similar points.
        The clusters found here will be used to reach the requirement "Trips with similar origin, 
        destination, and time of day should be grouped together".
        """
        kms_per_radian = 6371.0088
        max_dist_in_km = 1.6
        epsilon = max_dist_in_km / kms_per_radian # max distance that points can be from each other to be considered a cluster

        orig_coords = self._df[["orig_lat", "orig_lon"]].values
        orig_db = DBSCAN(eps=epsilon, min_samples=1, algorithm='ball_tree', metric='haversine')
        orig_db.fit(np.radians(orig_coords))
        self._df["orig_cluster"] = orig_db.labels_

        dest_coords = self._df[["dest_lat", "dest_lon"]].values
        dest_db = DBSCAN(eps=epsilon, min_samples=1, algorithm='ball_tree', metric='haversine')
        dest_db.fit(np.radians(dest_coords))
        self._df["dest_cluster"] = dest_db.labels_


    def _concat_orig_dest_timeday(self):
        """
        This functions just concatenates the groups/clusters of origin/destination
        coordinates with the time of day column. This will allow to group trips
        that are similar in terms of origin, destination and time of day.
        """
        self._df["trip_group"] = (
                "orig_" + self._df["orig_cluster"].astype(str) + 
                "-dest_" + self._df["dest_cluster"].astype(str) + 
                "-" + self._df["time_day"]
            )


    def create_db_indexes(self):
        """
        After loading all data into database, creates some indexes that will
        improve the performance of the queries that will be executed to get
        the weekly average number of trips.
        """
        self._con.execute("CREATE INDEX ix_trips_region ON trips(region);")
        self._con.execute("CREATE INDEX ix_trips_coord ON trips(orig_lat, orig_lon, dest_lat, dest_lon);")


    def get_weekly_avg_qt_trips_by_region(self, region):
        """
        Queries the database to find the weekly average number of trips
        filtering a region.
        """
        cur = self._con.cursor()
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
        res = cur.execute(query)
        result = res.fetchone()
        return result[1] if result is not None else None


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

        cur = self._con.cursor()
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
        res = cur.execute(query)
        result = res.fetchone()
        return result[0] if result is not None else None

