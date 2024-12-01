import time
import report_pb2
import weather

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError

broker = "localhost:9092"
topic_name = "temperatures"

admin_client = KafkaAdminClient(bootstrap_servers=[broker])
try:
    admin_client.delete_topics([topic_name])
    print("Deleted topics successfully")
    time.sleep(3)
except UnknownTopicOrPartitionError:
    print("Cannot delete topic/s (may not exist yet)")

topic = NewTopic(name=topic_name, num_partitions=4, replication_factor=1)
admin_client.create_topics([topic])
print("Created topic: '{}' successfully!".format(topic_name))
print("Topics:", admin_client.list_topics())


producer = KafkaProducer(
    bootstrap_servers=[broker],
    retries=10,
    acks="all",
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: v.SerializeToString(),
)
print("Initialized Producer successfully!")


for date, degrees, station_id in weather.get_next_weather(delay_sec=0.1):
    report = report_pb2.Report()
    report.date = date
    report.degrees = degrees
    report.station_id = station_id

    producer.send(
        topic=topic_name,
        key=station_id,
        value=report,
    )
    print(
        "Producer Record: date={}, degrees={}, station_id={}".format(
            date, degrees, station_id
        )
    )
    time.sleep(0.1)
producer.close()
