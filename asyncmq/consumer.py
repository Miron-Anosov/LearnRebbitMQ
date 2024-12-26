import asyncio
import logging
import random
import sys
import os

from aio_pika.abc import AbstractIncomingMessage

from asyncmq.worker import QueueRabbitClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def process_message(message: AbstractIncomingMessage):
    tag: int = 0
    if message.delivery_tag:
        tag = message.delivery_tag
    long_task = random.randint(0, 4)
    await asyncio.sleep(long_task)
    if long_task == 0:
        logger.warning("ЗАПРОС НЕ БЫЛ УСПЕШНО ОБРАБОТАН И НЕ был возвращен в очередь (requeue=False).")
        await message.reject(requeue=False)  # сообщение будет отброшено
    elif long_task == 1:
        await message.reject(requeue=True)  # сообщение вернётся в очередь
        logger.warning("ЗАПРОС НЕ БЫЛ УСПЕШНО ОБРАБОТАН И  НЕ был возвращен в очередь (requeue=False).")

    elif long_task == 2:
        logger.warning("ЗАПРОС НЕ БЫЛ УСПЕШНО ОБРАБОТАН И был возвращен в очередь (requeue=True).")
        await message.nack(requeue=True)  # похоже на reject, но можно использовать для нескольких сообщений

    elif long_task == 3:
        logger.warning("ЗАПРОС НЕ БЫЛ УСПЕШНО ОБРАБОТАН И  НЕ был возвращен в очередь (requeue=False).")
        await message.nack(requeue=False)  # вернёт сообщение в очередь

    else:

        await message.channel.basic_ack(delivery_tag=tag)  # подтверждает что сообщение успешно обработано
        # message.delivery_tag для чего нужны тэги и как их использовать?
        logger.info(f"Сообщение обработано: {message.body.decode()}, spent time: {long_task}")


async def main():
    async with QueueRabbitClient(amqp_url="amqp://user:password@localhost/") as client:

        # Объявляем обменник и очередь
        exchange = await client.declare_exchange("main-exchange", durable=True)
        queue = await client.declare_queue("main-queue", durable=True,             arguments={
                "x-dead-letter-exchange": "dead-letter-exchange",
                "x-dead-letter-routing-key": "dead-letter-queue",
                # "x-message-ttl": 120_000,  # 120 seconds TTL
            }
        )
        await client.channel.set_qos(prefetch_count=1)

        # Привязываем очередь к обменнику
        await client.bind_queue(queue, exchange.name, routing_key="main-queue")

        # Начинаем обрабатывать сообщения
        logger.info("Waiting for messages...")
        try:
            await client.consume(queue, process_message)
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
