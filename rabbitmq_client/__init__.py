import pika


class RabbitmqClient:
    def __init__(self, url):
        self._connection = pika.BlockingConnection(pika.URLParameters(url))
        self._channel = self._connection.channel()

    def get_channel(self):
        return self._channel

    def pull(self, callback, queue, durable=True):
        """
        Callback params : channel, method, properties, body
        """
        self._channel.queue_declare(queue=queue, durable=durable)
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue, on_message_callback=callback)
        self._channel.start_consuming()

    def get(self, queue, durable=True):
        self._channel.queue_declare(queue=queue, durable=durable)
        self._channel.basic_qos(prefetch_count=1)
        method, _, body = self._channel.basic_get(queue=queue)
        return body, method

    def push(self, message, routing_key, exchange='', delivery_mode=2):
        self._channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=delivery_mode,
            ))

    def delete(self, queue, delivery_tag, ack):
        if ack:
            self._channel.basic_ack(delivery_tag=delivery_tag)
        else:
            self._channel.basic_nack(delivery_tag=delivery_tag)
