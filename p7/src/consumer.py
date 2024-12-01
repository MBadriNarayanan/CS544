import json
import os
import sys

import report_pb2

from kafka import KafkaConsumer, TopicPartition

broker = "localhost:9092"
topic_name = "temperatures"


def load_data(part):
    json_file_name = os.path.join("/src/partition-{}.json".format(part))
    if os.path.exists(json_file_name):
        with open(json_file_name, "r") as json_file:
            return json.load(json_file), json_file_name
    return {"offset": 0}, json_file_name


def save_data(data, json_file_name):
    temp_file_name = json_file_name + ".tmp"
    with open(temp_file_name, "w") as json_file:
        json.dump(data, json_file, indent=4)
    os.rename(temp_file_name, json_file_name)


partitions = list(map(int, sys.argv[1:]))
if not partitions:
    print("Usage: python3 consumer.py <partition1> <partition2> ...")
    sys.exit(1)


consumer = KafkaConsumer(
    bootstrap_servers=[broker],
    enable_auto_commit=False,
    value_deserializer=lambda v: report_pb2.Report.FromString(v),
)

values = [TopicPartition(topic_name, p) for p in partitions]
consumer.assign(values)

partition_data = {}
for part in partitions:
    data, json_file_name = load_data(part=part)
    partition_data[part] = {"data": data, "json_file_name": json_file_name}
    offset = data["offset"]
    consumer.seek(TopicPartition(topic_name, part), offset)

for message in consumer:
    part = message.partition
    report = message.value
    station_id = report.station_id

    if station_id not in partition_data[part]["data"]:
        partition_data[part]["data"][station_id] = {
            "count": 0,
            "sum": 0.0,
            "avg": 0.0,
            "start": report.date,
            "end": report.date,
        }

    station_stats = partition_data[part]["data"][station_id]
    station_stats["count"] += 1
    station_stats["sum"] += report.degrees
    station_stats["avg"] = station_stats["sum"] / station_stats["count"]
    station_stats["start"] = min(station_stats["start"], report.date)
    station_stats["end"] = max(station_stats["end"], report.date)

    partition_data[part]["data"]["offset"] = consumer.position(
        TopicPartition("temperatures", part)
    )
    save_data(
        data=partition_data[part]["data"],
        json_file_name=partition_data[part]["json_file_name"],
    )
