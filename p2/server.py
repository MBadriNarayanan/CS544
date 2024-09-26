import grpc
import socket
import sys

from concurrent import futures
from matchdb_pb2 import GetMatchCountResp
from matchdb_pb2_grpc import add_MatchCountServicer_to_server, MatchCountServicer

import pandas as pd


def determine_partition():
    container_ip = socket.gethostbyname(socket.gethostname())
    server1_ip = socket.gethostbyname("wins-server-1")
    server2_ip = socket.gethostbyname("wins-server-2")

    if container_ip == server1_ip:
        return "partitions/part_{}.csv".format(0)
    elif container_ip == server2_ip:
        return "partitions/part_{}.csv".format(1)


class MatchCountService(MatchCountServicer):
    def __init__(self, data):
        self.data = data

    def GetMatchCount(self, request, context):
        count = 0
        for _, row in self.data.iterrows():
            winning_team = row.get("winning_team", "")
            if winning_team != winning_team:
                winning_team = ""
            else:
                winning_team = str(winning_team).strip()

            country = row.get("country", "")
            if country != country:
                country = ""
            else:
                country = str(country).strip()

            if (request.country == "" or country == request.country) and (
                request.winning_team == "" or winning_team == request.winning_team
            ):
                count += 1
        return GetMatchCountResp(num_matches=count)


def serve(data, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_MatchCountServicer_to_server(MatchCountService(data), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print("Server has been started on port: {}".format(port))
    server.wait_for_termination()


if __name__ == "__main__":
    if len(sys.argv) > 2:
        print("Usage: python server.py <PATH_TO_CSV_FILE> <PORT>")
        csv_path = sys.argv[1]
        port = int(sys.argv[2])
    elif len(sys.argv) > 1:
        print("Usage: python server.py <PATH_TO_CSV_FILE>")
        csv_path = sys.argv[1]
        port = 5440
    else:
        print("Usage: python server.py")
        csv_path = determine_partition()
        port = 5440

    print("CSV Path: {}".format(csv_path))
    print("Port: {}".format(port))

    df = pd.read_csv(csv_path)
    serve(data=df, port=port)
