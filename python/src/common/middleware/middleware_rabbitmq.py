import pika

from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self.queue_name)

    def close(self):
        if self._connection and self._connection.is_open:
            self._connection.close()

    def send(self, message):
        pass

    def start_consuming(self, on_message_callback):
        pass

    def stop_consuming(self):
        pass

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self._channel = self._connection.channel()
        self._channel.exchange_declare(exchange=self.exchange_name, exchange_type="direct")
        self._consumer_queue = None
        self._should_consume = True

    def close(self):
        if self._connection and self._connection.is_open:
            self._connection.close()

    def send(self, message):
        for routing_key in self.routing_keys:
            self._channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=message,
            )

    def start_consuming(self, on_message_callback):
        self._should_consume = True
        result = self._channel.queue_declare(queue="", exclusive=True)
        self._consumer_queue = result.method.queue

        for routing_key in self.routing_keys:
            self._channel.queue_bind(
                exchange=self.exchange_name,
                queue=self._consumer_queue,
                routing_key=routing_key,
            )

        def _on_message(channel, method, properties, body):
            if not self._should_consume:
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                channel.stop_consuming()
                return

            def _ack():
                channel.basic_ack(delivery_tag=method.delivery_tag)

            def _nack():
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            on_message_callback(body, _ack, _nack)

        self._channel.basic_consume(
            queue=self._consumer_queue,
            on_message_callback=_on_message,
            auto_ack=False,
        )
        self._channel.start_consuming()

    def stop_consuming(self):
        self._should_consume = False
        if self._channel and self._channel.is_open:
            self._channel.stop_consuming()
