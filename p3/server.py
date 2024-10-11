import grpc
import table_pb2_grpc
import table_pb2
import pytz

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from concurrent import futures
from datetime import datetime
from threading import Lock

file_store = {"MetaData": {}, "Index": -1}
lock = Lock()


def get_timestamp():
    timestamp = datetime.now(pytz.timezone("US/Central"))
    timestamp = timestamp.strftime("%H_%M_%S_%Y_%m_%d")
    return timestamp


class TableServicer(table_pb2_grpc.TableServicer):
    def Upload(self, request, context):
        global file_store, lock
        timestamp = get_timestamp()
        csv_path = "/data_{}.csv".format(timestamp)
        parquet_path = "/data_{}.parquet".format(timestamp)
        with open(csv_path, "wb") as csv_file:
            csv_file.write(request.csv_data)
        try:
            df = pd.read_csv(csv_path)
        except Exception as error:
            return table_pb2.UploadResp(
                error="Error processing CSV: {}".format(str(error))
            )

        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_path)

        with lock:
            meta_data = {
                "Timestamp": timestamp,
                "CSV": csv_path,
                "Parquet": parquet_path,
            }
            file_store["Index"] += 1
            file_store["MetaData"][file_store["Index"]] = meta_data

        return table_pb2.UploadResp(error="")

    def ColSum(self, request, context):
        global file_store, lock
        total_sum = 0
        column = request.column
        format = request.format.lower()

        with lock:
            file_store_copy = file_store.copy()

        for _, meta_data in file_store_copy["MetaData"].items():
            try:
                if format == "csv":
                    csv_path = meta_data["CSV"]
                    df = pd.read_csv(csv_path)
                    if column in df.columns:
                        total_sum += df[column].sum()
                elif format == "parquet":
                    parquet_path = meta_data["Parquet"]
                    table = pq.read_table(parquet_path, columns=[column])
                    total_sum += table.column(column).to_pandas().sum()
            except Exception as error:
                continue
        return table_pb2.ColSumResp(total=total_sum, error="")


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
