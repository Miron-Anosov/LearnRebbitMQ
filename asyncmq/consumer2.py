import asyncio
import logging
import sys
import os
import random

from aio_pika.abc import AbstractIncomingMessage

from asyncmq.worker import QueueRabbitClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def process_message(message: AbstractIncomingMessage):
    long_task = random.randint(0, 3)
    await asyncio.sleep(long_task)
    logger.info(f"Received message: {message.body.decode()}, spent time: {long_task}")


async def main():
    async with QueueRabbitClient(amqp_url="amqp://user:password@localhost/") as client:

        # Объявляем обменник и очередь
        exchange = await client.declare_exchange("test_exchange", durable=True)

        queue = await client.declare_queue("test_queue", durable=True)
        await client.channel.set_qos(prefetch_count=1)

        # Привязываем очередь к обменнику
        await client.bind_queue(queue, exchange.name, routing_key="test_key")

        # Начинаем обрабатывать сообщения
        logger.info("Waiting for messages...")
        try:
            await client.consume(queue, process_message, auto_ack=True)
        except KeyboardInterrupt:
            logger.info("Consumer stopped.")


if __name__ == '__main__':
    try:
        logging.basicConfig(level=logging.INFO)
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)  # noqa
