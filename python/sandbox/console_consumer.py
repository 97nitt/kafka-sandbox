from sandbox.avro import schema
from sandbox.kafka.consumer import Consumer, AvroConsumer
import argparse
import logging
import os
import signal
import uuid

# setup logging
logging.basicConfig(
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    level=os.environ.get('LOG_LEVEL', 'WARN').upper())

logger = logging.getLogger(__name__)

# setup command line arguments
parser = argparse.ArgumentParser(
    description="Kafka console consumer.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("--brokers",
                    help="Kafka Broker URLs",
                    default="localhost:9092")
parser.add_argument("--topic",
                    help="Kafka topic",
                    required=True)
parser.add_argument("--group",
                    help="Kafka consumer group")
parser.add_argument("--from-earliest",
                    help="If no committed offsets are available, start from earliest",
                    action="store_true")
parser.add_argument("--avro",
                    help="Serialize messages using Avro",
                    action="store_true")
parser.add_argument("--schema",
                    help="Path to Avro schema file",
                    metavar="FILE")
parser.add_argument("--registry",
                    help="Scham Registry URL",
                    default="http://localhost:8081",
                    metavar="URL")

# parse command line arguments
args = parser.parse_args()
if args.avro:
    if not args.schema:
        parser.error("--schema is required when using --avro")
    if not args.registry:
        parser.error("--registry is required when using --avro")

# initialize counter to track messages received
messages_received = 0

# message handler prints message and increments counter
def message_handler(message):
    global messages_received
    messages_received += 1
    print(f"key={message.key()}, "
          f"value={message.value()}, "
          f"timestamp={message.timestamp()[1]}, "
          f"partition={message.partition()}, "
          f"offset={message.offset()}")

# SIGINT handler
def sigint_handler(signal, frame):
    print()
    print(f"Total messages received: {messages_received}")
    exit(0)

signal.signal(signal.SIGINT, sigint_handler)

# create Kafka consumer
group = args.group if args.group else f"console-{uuid.uuid4()}"
config = None
if (args.from_earliest):
    config = {
        "auto.offset.reset": "earliest"
    }

if args.avro:
    consumer = AvroConsumer(
        args.brokers,
        args.topic,
        group,
        message_handler,
        args.registry,
        schema.from_file(args.schema),
        config=config)
else:
    consumer = Consumer(
        args.brokers,
        args.topic,
        group,
        message_handler,
        config=config)

print(f"Starting consumer (brokers={args.brokers}, topic={args.topic}, group={group}).")
print("Use Ctrl-C to exit.")
consumer.start()
