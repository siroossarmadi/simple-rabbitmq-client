import pika
import pickle


class RabbitmqClient:
    def __init__(self, host):
        self.host = host
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()

    def pull(self, callback, queue, durable=True):
        self.channel.queue_declare(queue=queue, durable=durable)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue, on_message_callback=callback)
        self.channel.start_consuming()

    def get(self, queue, durable=True):
        self.channel.queue_declare(queue=queue, durable=durable)
        self.channel.basic_qos(prefetch_count=1)
        method, _, body = self.channel.basic_get(queue=queue)
        return body, method.delivery_tag

    def push(self, message, routing_key, exchange='', delivery_mode=2):
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=delivery_mode,
            ))

    def delete(self, queue, delivery_tag, ack):
        if ack:
            self.channel.basic_ack(delivery_tag=delivery_tag)
        else:
            self.channel.basic_nack(delivery_tag=delivery_tag)
