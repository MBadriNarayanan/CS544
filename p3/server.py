import grpc
import table_pb2_grpc
import table_pb2
import pytz
import uuid

import pyarrow.csv as csv
import pyarrow.parquet as pq

from concurrent import futures
from datetime import datetime
from threading import Lock

file_store = {}
lock = Lock()


def get_timestamp():
    timestamp = datetime.now(pytz.timezone("US/Central"))
    timestamp = timestamp.strftime("%H_%M_%S_%Y_%m_%d")
    return timestamp


class TableServicer(table_pb2_grpc.TableServicer):
    def Upload(self, request, context):
        file_id = str(uuid.uuid4())
        timestamp = get_timestamp()
        csv_path = "{}_{}.csv".format(file_id, timestamp)
        parquet_path = "{}_{}.parquet".format(file_id, timestamp)

        try:
            with open(csv_path, "wb") as csv_file:
                csv_file.write(request.csv_data)

            table = csv.read_csv(csv_path)
            pq.write_table(table, parquet_path)

            with lock:
                file_store["{}_{}".format(file_id, timestamp)] = {
                    "csv": csv_path,
                    "parquet": parquet_path,
                }
            return table_pb2.UploadResp(error="")
        except Exception as error:
            return table_pb2.UploadResp(error=str(error))

    def ColSum(self, request, context):
        total_sum = 0
        column = request.column
        request_format = request.format.lower()

        try:
            with lock:
                file_store_copy = file_store.copy()

            for file_key, meta_data in file_store_copy.items():
                try:
                    if request_format == "csv":
                        table = csv.read_csv(meta_data["csv"])
                        if column in table.column_names:
                            column_data = table[request.column]
                            for chunk in column_data.chunks:
                                total_sum += sum(chunk.to_pylist())
                    elif request_format == "parquet":
                        parquet_meta_data = pq.read_metadata(meta_data["parquet"])
                        if request.column in parquet_meta_data.schema.names:
                            table = pq.read_table(meta_data["parquet"], columns=[request.column])
                            column_data = table[request.column]
                            for chunk in column_data.chunks:
                                total_sum += sum(chunk.to_pylist())
                except Exception as error:
                    continue
            return table_pb2.ColSumResp(total=total_sum, error="")
        except Exception as error:
            return table_pb2.ColSumResp(error=str(error))


def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=8), options=[("grpc.so_reuseport", 0)]
    )
    table_pb2_grpc.add_TableServicer_to_server(TableServicer(), server)
    server.add_insecure_port("[::]:5440")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
