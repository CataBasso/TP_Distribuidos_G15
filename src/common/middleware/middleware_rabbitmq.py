import time
import pika

from .middleware import (
    MessageMiddlewareConnection,
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError,
)

class RabbitConnection(MessageMiddlewareConnection):
    """Una conexión y un canal. Varios endpoints del mismo proceso pueden compartirla
    para escuchar de varias colas (datos, ctrl, abort) en un único loop."""

    def __init__(self, host, prefetch=1, heartbeat=600, retries=10, delay=1.0, confirm=False):
        self.host = host
        self.confirm = confirm
        params = pika.ConnectionParameters(host=host, heartbeat=heartbeat, blocked_connection_timeout=300)
        last_error = None
        for _ in range(retries):
            try:
                self.connection = pika.BlockingConnection(params)
                break
            except pika.exceptions.AMQPConnectionError as e:
                last_error = e
                time.sleep(delay)
        else:
            raise MessageMiddlewareDisconnectedError(str(last_error))
        
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=prefetch)
        if confirm:
            self.channel.confirm_delivery()

    def run(self):
        try:
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e

    def stop(self):
        try:
            self.connection.add_callback_threadsafe(self.channel.stop_consuming)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e

    def close(self):
        try:
            if self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e)) from e


class _RabbitEndpoint:

    def _init_connection(self, host, connection):
        self.host = host
        self._own_connection = connection is None
        self.rc = connection if connection is not None else RabbitConnection(host)
        self.channel = self.rc.channel
        self._consumer_tag = None
        self._consuming = False

    def _queue_to_consume(self):
        raise NotImplementedError

    def consume(self, on_message_callback):
        def callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack(requeue=True):
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=requeue)

            on_message_callback(body, ack, nack)

        try:
            queue = self._queue_to_consume()
            self._consumer_tag = self.channel.basic_consume(queue=queue, on_message_callback=callback)
            self._consuming = True
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e

    def start_consuming(self, on_message_callback):
        self.consume(on_message_callback)
        try:
            self.rc.run()
        finally:
            self._consuming = False

    def stop_consuming(self):
        if not self._consuming:
            return
        self._consuming = False
        try:
            if self._own_connection:
                self.rc.stop()
            else:
                tag = self._consumer_tag
                self.rc.connection.add_callback_threadsafe(lambda: self.channel.basic_cancel(tag))
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e

    def close(self):
        if self._own_connection:
            self.rc.close()


class MessageMiddlewareQueueRabbitMQ(_RabbitEndpoint, MessageMiddlewareQueue):

    def __init__(self, host, queue_name, connection=None, durable=True):
        self.queue_name = queue_name
        self.durable = durable
        try:
            self._init_connection(host, connection)
            self.channel.queue_declare(queue=queue_name, durable=durable)
        except MessageMiddlewareDisconnectedError:
            raise
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e

    def _queue_to_consume(self):
        return self.queue_name

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange='', routing_key=self.queue_name, body=message,
                properties=pika.BasicProperties(delivery_mode=2 if self.durable else 1),
                mandatory=self.rc.confirm)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e

    def delete(self):
        try:
            self.channel.queue_delete(queue=self.queue_name)
        except Exception as e:
            raise MessageMiddlewareDeleteError(str(e)) from e


class MessageMiddlewareExchangeRabbitMQ(_RabbitEndpoint, MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys, exchange_type='direct',
                                queue_name=None, durable=True, connection=None):
        self.exchange_name = exchange_name
        self.routing_keys = list(routing_keys)
        self.exchange_type = exchange_type
        self.queue_name = queue_name
        self.durable = durable
        self._anon_queue = None
        try:
            self._init_connection(host, connection)
            self.channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type,
                                          durable=durable)
            if queue_name is not None:
                self.channel.queue_declare(queue=queue_name, durable=durable)
                self._bind(queue_name)
        except MessageMiddlewareDisconnectedError:
            raise
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e

    def _bind(self, queue):
        keys = [''] if self.exchange_type == 'fanout' else self.routing_keys
        for key in keys:
            self.channel.queue_bind(exchange=self.exchange_name, queue=queue, routing_key=key)

    def _queue_to_consume(self):
        if self.queue_name is not None:
            return self.queue_name
        if self.exchange_type == 'direct' and not self.routing_keys:
            raise MessageMiddlewareMessageError("exchange direct sin routing keys para consumir")
        if self._anon_queue is None:
            self._anon_queue = self.channel.queue_declare(queue='', exclusive=True).method.queue
            self._bind(self._anon_queue)
        return self._anon_queue

    def send(self, message, routing_key=None):
        if routing_key is None:
            routing_key = self.routing_keys[0] if self.routing_keys else ''
        try:
            self.channel.basic_publish(
                exchange=self.exchange_name, routing_key=routing_key, body=message,
                properties=pika.BasicProperties(delivery_mode=2 if self.durable else 1),
                mandatory=self.rc.confirm)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e

    def delete(self):
        try:
            if self.queue_name is not None:
                self.channel.queue_delete(queue=self.queue_name)
            self.channel.exchange_delete(exchange=self.exchange_name)
        except Exception as e:
            raise MessageMiddlewareDeleteError(str(e)) from e