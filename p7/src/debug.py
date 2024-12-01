from kafka import KafkaConsumer
import report_pb2

broker = "localhost:9092"
topic_name = "temperatures"

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[broker],
    group_id="debug",
    auto_offset_reset="latest",
    value_deserializer=lambda v: report_pb2.Report.FromString(v),
)
for message in consumer:
    report = message.value
    result = {
        "station_id": report.station_id,
        "date": report.date,
        "degrees": report.degrees,
        "partition": message.partition,
    }
    print(result)
