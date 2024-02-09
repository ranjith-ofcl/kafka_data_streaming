from kafka import KafkaConsumer

bootstrap_servers = ['localhost:29092']

consumer = KafkaConsumer(
    bootstrap_servers = bootstrap_servers,
    group_id = None,
    max_poll_records=2
    )

print(consumer.topics())