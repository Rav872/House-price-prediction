from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

topic = 'house-prices'
for i in range(10):
    data = f'house price {i}'
    producer.produce(topic, key=str(i), value=data, callback=delivery_report)
    producer.poll(0)

producer.flush()