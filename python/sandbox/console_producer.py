from sandbox.avro import schema
from sandbox.kafka.producer import Producer, AvroProducer
import argparse
import logging
import os
import signal

# setup logging
logging.basicConfig(
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    level=os.environ.get('LOG_LEVEL', 'WARN').upper())

logger = logging.getLogger(__name__)

# setup command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--brokers", help="Kafka Broker URLs", default="localhost:9092")
parser.add_argument("--topic", help="Kafka topic", required=True)
parser.add_argument("--avro", help="Serialize messages using Avro", action="store_true")
parser.add_argument("--schema", help="Path to Avro schema file")
parser.add_argument("--schema-registry", help="Scham Registry URL", default="http://localhost:8081")

# parse command line arguments
args = parser.parse_args()
if args.avro:
    if not args.schema:
        parser.error("--schema is required when using --avro")
    if not args.schema_registry:
        parser.error("--schema-registry is required when using --avro")

# initialize counters to track messages sent
messages_sent = 0
messages_delivered = 0
messages_failed = 0

# producer callback to increment counters
def producer_callback(error, message):
    global messages_delivered, messages_failed
    if error is not None:
        messages_failed += 1
        logger.error(f"Message delivery failed: {error}")
    else:
        messages_delivered += 1
        logger.debug(f"Message successfully delivered: "
                     f"topic={message.topic()}, "
                     f"partition={message.partition()}, "
                     f"offset={message.offset()}, "
                     f"timestamp={message.timestamp()[1]}, "
                     f"key={message.key()}, "
                     f"value={message.value()}")

# SIGINT handler
def sigint_handler(signal, frame):
    print()
    print(f"Total messages sent: {messages_sent}")
    print(f"Total messages delivered: {messages_delivered}")
    print(f"Total messages failed: {messages_failed}")
    exit(0)

signal.signal(signal.SIGINT, sigint_handler)

# create Kafka producer
if args.avro:
    producer = AvroProducer(
        args.brokers,
        args.topic,
        args.schema_registry,
        schema.from_file(args.schema))
else:
    producer = Producer(args.brokers, args.topic)

# send messages
print(f"Starting producer (brokers={args.brokers}, topic={args.topic}).")
print("Enter message text below. Press enter/return to send message, Ctrl-C to exit.")
while(True):
    message = input()
    producer.send(value=message, on_delivery=producer_callback)
    messages_sent += 1
