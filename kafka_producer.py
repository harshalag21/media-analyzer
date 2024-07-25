from configparser import ConfigParser
from kafka import KafkaProducer


class Kafka:
    def __init__(self):
        """
        Initialize Kafka producer class
        """
        # Config parsing
        self._config = ConfigParser()
        self._config.read(["./config.ini"])
        self._bootstrap_servers = self._config.get("KAFKA", "bootstrap_servers")

        try:
            # Initialise Kafka producer instance
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=[self._bootstrap_servers],
                linger_ms=10000)
            print(f"Connected to Kafka@{self._bootstrap_servers}")
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}")

    def publish(self, text, topic):
        """
        Publish message to Kafka topic
        :param text: message to be published
        :param topic: kafka topic name
        :return:
        """
        try:
            # Encode message
            key_bytes = bytes('post', encoding='utf-8')
            value_bytes = bytes(text, encoding='utf-8')
            # Publish to Kafka
            self.kafka_producer.send(
                topic=topic,
                key=key_bytes,
                value=value_bytes)
            self.kafka_producer.flush()
        except Exception as e:
            print(f'Failed to publish message: {e}')