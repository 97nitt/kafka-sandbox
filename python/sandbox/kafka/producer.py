from confluent_kafka import Producer

def callback(err, msg):
    """ Called once for each message produced to indicate delivery success or failure.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message successfully delivered: "
              f"topic={msg.topic()}, "
              f"partition={msg.partition()}, "
              f"offset={msg.offset()}, "
              f"timestamp={msg.timestamp()[1]}, "
              f"key={msg.key()}, "
              f"value={msg.value()}")

# create producer
producer = Producer({
    'bootstrap.servers': 'localhost:9092'
})

# send a message
producer.produce('test-topic', 'test', callback=callback)

# wait for all queued messages to be delivered
producer.flush()
