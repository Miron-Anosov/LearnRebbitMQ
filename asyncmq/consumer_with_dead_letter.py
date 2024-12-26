import asyncio
import logging
import random

from aio_pika.abc import AbstractIncomingMessage

from asyncmq.worker import DeadLetterQueueClient

logger = logging.getLogger(__name__)


async def process_message(message: AbstractIncomingMessage):
    """Обработчик для основной очереди"""
    try:
        data = message.body.decode()
        logging.info(f"Обработка сообщения: {data}")

        if random.random() > 0.5:
            raise Exception("Симуляция ошибки")

        # Если всё ок, сообщение будет подтверждено автоматически
        # благодаря контекстному менеджеру в методе consume
        logger.info(f"Успешно обработано сообщение.")

    except Exception as e:
        # При возникновении ошибки сообщение будет отправлено в DLX
        # НЕ используем requeue=True, чтобы сообщение ушло в DLX
        await message.nack(requeue=False)
        raise


async def process_dead_letter(message: AbstractIncomingMessage):
    """Обработчик для dead letter очереди"""
    try:
        data = message.body.decode()
        logging.warning(f"Обработка сообщения из DLX: {data}")
        # Здесь может быть специальная логика обработки "мертвых" сообщений
        await message.ack() # подтверждаем выполнение сообщения.

    except Exception as e:
        logging.error(f"Ошибка обработки DLX сообщения: {e}")
        await message.nack(requeue=True)  # Можно решить, нужен ли requeue
        raise


async def main():
    mq_url = "amqp://user:password@localhost/"
    client = DeadLetterQueueClient(amqp_url=mq_url)
    async with client:
        await client.run(process_message, process_dead_letter)


if __name__ == '__main__':
    asyncio.run(main())