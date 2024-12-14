import logging
import os
import sys
import time
from typing import Callable

import random
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from consumers_models.consumer_base import RabbitMQClientBase

logger = logging.getLogger(__name__)


def process_new_msg(
        channel: "BlockingChannel",
        method: "Basic.Deliver",
        properties: "BasicProperties",
        body: bytes,
):
    # Запускаем задачу
    expensive_task = random.randint(1, 9)

    time.sleep(expensive_task)
    logging.debug("Chanel %s", channel)
    logging.debug("Method %s", method)
    logging.debug("Properties %s", properties)
    logging.info("Body %s", body)

    if random.random() > 0.5:

        logger.warning("ЗАПРОС НЕ БЫЛ УСПЕШНО ОБРАБОТАН И был возвращен в очередь (requeue=True).")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    elif random.random() > 0.7:

        logger.warning("ЗАПРОС НЕ БЫЛ УСПЕШНО ОБРАБОТАН И  НЕ был возвращен в очередь (requeue=False).")
        channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

    else:

        logger.info("ЗАПРОС БЫЛ УСПЕШНО ОБРАБОТАН.")
        channel.basic_ack(delivery_tag=method.delivery_tag)  # type: ignore # подтверждаем выполнение задачи.

    logger.warning("Work finished for %d seconds", expensive_task)


def consume_messages(
        channel: BlockingChannel,
        queue_name: str,
        on_message_callback: Callable,
        auto_ack: bool = False,  # подтверждаем выполнение задачи автоматически если True.
) -> None:
    """Обрабатывает сообщения из очереди."""
    channel.basic_qos(prefetch_count=2)  # лимитируем сообщения из очереди.
    # Удаление устойчивой очереди
    channel.queue_delete(queue='test')

    # Удаление устойчивого обменника. В Данном примере удаление стоит перед созданием очереди. НО если нужен устойчевый канал или обменник, тогда удаление тут будет противоречиво и устойчивая очереди или удаленник не будут таковыми являться.
    channel.exchange_delete(exchange='your_exchange_name')
    queue_ = channel.queue_declare(
        queue="test",  # Создаем очередь с нужным названием.
        durable=False,  #Создаём долговечную очередь. При перезапуске очередь будет востановлена из диска(снижается производительность)
        passive=False, # Если очередь не существует, вызывается исключение
                       # Если очередь существует, никаких действий не происходит.
                       # Не создает новую очередь, если она отсутствует
        # auto_delete=False,
        # exclusive=False
    )
    logger.info("Created queue = %s, routing_key = %s", queue_, "test")

    # Устанавливаем количество сообщений в очереди.
    # Очередь будет передаваться другому consumer, если очередь данного будет заполнена.
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=on_message_callback,
        auto_ack=auto_ack,

    )
    logger.info("Waiting for messages %s", queue_name)
    channel.start_consuming()


def main() -> None:
    config_logging()
    # connection = get_connection()

    with RabbitMQClientBase() as mq_client:
        logger.info("Connected to RabbitMQ server %s", mq_client)
        logger.info("Channel is opening %s", mq_client.channel)
        # Передаем задачу обработчику заданий очереди.
        consume_messages(
            channel=mq_client.channel,
            on_message_callback=process_new_msg,
            queue_name="test",
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
            os._exit(0)  # noqa
