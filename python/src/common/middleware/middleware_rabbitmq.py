import pika
from .middleware import (
    MessageMiddlewareQueue, 
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError
)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._host = host
        self._queue_name = queue_name
        self._connection = None
        self._channel = None
        self._consuming = False
        self._connect()

    def _connect(self):
        try:
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._host))
            self._channel = self._connection.channel()
            self._channel.queue_declare(queue=self._queue_name)
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to connect to RabbitMQ: {e}")

    def start_consuming(self, on_message_callback):
        
        def callback(ch, method, body):
            try:
                def ack():
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                
                def nack():
                    ch.basic_nack(delivery_tag=method.delivery_tag)
                
                on_message_callback(body, ack, nack)
            except Exception as e:
                raise MessageMiddlewareMessageError(f"Error in message callback: {e}")

        try:
            self._channel.basic_consume(queue=self._queue_name, on_message_callback=callback)
            self._consuming = True
            self._channel.start_consuming()
        except pika.exceptions.ConnectionClosed:
            raise MessageMiddlewareDisconnectedError("Connection lost while consuming")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error while consuming: {e}")

    def stop_consuming(self):
        if self._consuming and self._channel and not self._channel.is_closed:
            try:
                self._channel.stop_consuming()
                self._consuming = False
            except pika.exceptions.ConnectionClosed:
                raise MessageMiddlewareDisconnectedError("Connection lost while stopping consumption")
            except Exception as e:
                raise MessageMiddlewareMessageError(f"Error while stopping consumption: {e}")

    def send(self, message):
        try:
            self._channel.basic_publish(
                exchange='',
                routing_key=self._queue_name,
                body=message
            )
        except pika.exceptions.ConnectionClosed:
            raise MessageMiddlewareDisconnectedError("Connection lost while sending message")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error while sending message: {e}")

    def close(self):
        try:
            if self._consuming:
                self.stop_consuming()
            if self._channel and not self._channel.is_closed:
                self._channel.close()
            if self._connection and not self._connection.is_closed:
                self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error while closing connection: {e}")

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
