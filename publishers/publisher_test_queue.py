import json
import logging
import sys
import os

import pika
from pika.adapters.blocking_connection import BlockingChannel

from consumers_models.consumer_base import RabbitMQClientBase
from rabbitmq_conf import config_logging

logger = logging.getLogger(__name__)


def declare_queue(channel: "BlockingChannel", ) -> None:
    queue_ = channel.queue_declare(
        queue="test",  # Создаем очередь с нужным названием.
        durable=True # Создаём долговечную очередь. При перезапуске очередь будет востановлена из диска(снижается производительность)
    )
    logger.info("Created queue = %s, routing_key = %s", queue_, "test")


def produce_message(
        channel: "BlockingChannel",
        exchange,
        routing_key,
        body,
    index: int
):
    """Producer."""
    message = {
        f"message-{index:02d}": body,
    }
    body_to_queue = json.dumps(message)
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=body_to_queue.encode(),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,  # type: ignore # Делает сообщение persistent, не пропадают, если сервер перезагрузится.
            expiration='5000'  # TTL сообщения (в миллисекундах)
        )
    )
    logger.info("Сообщение отправлено в RabbitMQ : %s", body_to_queue)


def main() -> None:
    config_logging()
    with RabbitMQClientBase() as connection:
        logger.info("Connected to RabbitMQ server %s", connection)
        logger.info("Channel is opening %s", connection.channel)
        declare_queue(channel=connection.channel)
        for index in range(10):
            produce_message(
                channel=connection.channel,
                exchange="",
                routing_key="test",
                body="Hello world again!",
            index=index,
            )


if __name__ == '__main__':
    try:
        logging.basicConfig(level=logging.INFO)
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)  # type: ignore # noqa
