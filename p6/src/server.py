import grpc
import station_pb2
import station_pb2_grpc

from datetime import datetime
from cassandra import Unavailable
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.query import ConsistencyLevel
from concurrent import futures
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, trim


class StationService(station_pb2_grpc.StationServicer):
    def __init__(self):
        self.cluster = Cluster(["p6-db-1", "p6-db-2", "p6-db-3"])
        self.session = self.cluster.connect()

        self.session.execute("DROP KEYSPACE IF EXISTS weather")
        self.session.execute(
            """
            CREATE KEYSPACE weather 
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
        """
        )
        self.session.set_keyspace("weather")

        self.session.execute(
            """
            CREATE TYPE IF NOT EXISTS station_record (
                tmin int, 
                tmax int
            )
        """
        )
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS stations (
                id text,
                date date,
                name text static,
                record station_record,
                PRIMARY KEY (id, date)
            ) WITH CLUSTERING ORDER BY (date ASC)
        """
        )

        self.spark = SparkSession.builder.appName("p6").getOrCreate()

        data = self.spark.read.text("ghcnd-stations.txt")
        data = (
            data.withColumn("id", trim(substring(col("value"), 1, 11)))
            .withColumn("state", trim(substring(col("value"), 39, 2)))
            .withColumn("name", trim(substring(col("value"), 42, 30)))
            .select("id", "state", "name")
        )
        data = data.filter(col("state") == "WI").collect()

        for row in data:
            self.session.execute(
                """INSERT INTO weather.stations (id, name) VALUES (%s, %s)""",
                (row.id, row.name),
            )

        # ============ Server Stated Successfully =============
        print("Server started")  # Don't delete this line!

    def StationSchema(self, request, context):
        try:
            result = self.session.execute("DESCRIBE TABLE weather.stations")
            result = result.one().create_statement
            return station_pb2.StationSchemaReply(schema=result, error="")
        except Exception as error:
            return station_pb2.StationSchemaReply(schema="", error=str(error))

    def StationName(self, request, context):
        try:
            result = self.session.execute(
                """SELECT name FROM weather.stations WHERE id = %s""",
                (request.station,),
            )
            result = result.one()
            if result:
                name = result.name.strip()
                return station_pb2.StationNameReply(name=name, error="")
            else:
                return station_pb2.StationNameReply(name="", error="Station not found")
        except Exception as error:
            return station_pb2.StationNameReply(name="", error=str(error))

    def RecordTemps(self, request, context):
        try:
            prepared = self.session.prepare(
                """INSERT INTO weather.stations (id, date, record) 
                VALUES (?, ?, {tmin: ?, tmax: ?})"""
            )
            prepared.consistency_level = ConsistencyLevel.ONE

            self.session.execute(
                prepared,
                (
                    request.station,
                    datetime.strptime(request.date, "%Y-%m-%d").date(),
                    request.tmin,
                    request.tmax,
                ),
            )
            return station_pb2.RecordTempsReply(error="")
        except (Unavailable, NoHostAvailable):
            return station_pb2.RecordTempsReply(error="unavailable")
        except Exception as error:
            return station_pb2.RecordTempsReply(error="Error writing to database")

    def StationMax(self, request, context):
        try:
            prepared = self.session.prepare(
                """SELECT record FROM weather.stations WHERE id = ?"""
            )
            prepared.consistency_level = ConsistencyLevel.THREE

            result = self.session.execute(prepared, (request.station,))
            result = max(
                [
                    row.record.tmax
                    for row in result
                    if row.record and row.record.tmax is not None
                ],
                default=None,
            )
            if result is not None:
                return station_pb2.StationMaxReply(tmax=result, error="")
            else:
                return station_pb2.StationMaxReply(
                    tmax=0, error="Station not found or no records available."
                )
        except (Unavailable, NoHostAvailable):
            return station_pb2.StationMaxReply(tmax=-1, error="unavailable")
        except Exception as error:
            return station_pb2.StationMaxReply(
                tmax=-1, error="Error reading from database"
            )


def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=9),
        options=[("grpc.so_reuseport", 0)],
    )
    station_pb2_grpc.add_StationServicer_to_server(StationService(), server)
    server.add_insecure_port("0.0.0.0:5440")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
