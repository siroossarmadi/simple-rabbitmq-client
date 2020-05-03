import pika


class RabbitmqClient:
    def __init__(self, url):
        self.__connection = pika.BlockingConnection(pika.URLParameters(url))
        self.__channel = self.__connection.channel()

    def get_channel(self):
        return self.__channel

    def pull(self, callback, queue, durable=True):
        """
        Callback params : channel, method, properties, body
        """
        self.__channel.queue_declare(queue=queue, durable=durable)
        self.__channel.basic_qos(prefetch_count=1)
        self.__channel.basic_consume(queue, on_message_callback=callback)
        self.__channel.start_consuming()

    def get(self, queue, durable=True):
        self.__channel.queue_declare(queue=queue, durable=durable)
        self.__channel.basic_qos(prefetch_count=1)
        method, _, body = self.__channel.basic_get(queue=queue)
        return body, method

    def push(self, message, routing_key, exchange='', delivery_mode=2):
        self.__channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=delivery_mode,
            ))

    def delete(self, queue, delivery_tag, ack):
        if ack:
            self.__channel.basic_ack(delivery_tag=delivery_tag)
        else:
            self.__channel.basic_nack(delivery_tag=delivery_tag)
