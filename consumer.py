from confluent_kafka import Consumer

conf = {
    'bootstrap.servers': '10.0.1.5:9092',
    'group.id': 'house-price-group',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'PLAINTEXT',  # Ensure plain communication
}

consumer = Consumer(conf)
consumer.subscribe(['house-prices'])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue
    print(f"Received message: {msg.value().decode('utf-8')}")

consumer.close()