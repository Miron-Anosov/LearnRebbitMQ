import logging
import sys
import os
import time
import random

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from consumers_models.consumer_email_simple_dead_letter_exchange import MQDeadLetterExchangeLesson

from rabbitmq_conf import config_logging

# Настраиваем логгер для записи событий
logger = logging.getLogger(__name__)


def process_new_msg(
        channel: "BlockingChannel",
        method: "Basic.Deliver",
        properties: "BasicProperties",
        body: bytes,
):
    """
    Обработка входящих сообщений из очереди.

    Параметры:
        channel (BlockingChannel): Объект канала RabbitMQ.
        method (Basic.Deliver): Метод доставки сообщения.
        properties (BasicProperties): Дополнительные свойства сообщения.
        body (bytes): Тело сообщения.
    """
    # Логирование параметров сообщения для отладки
    # logging.warning("LEARN DEAD LETTERS EXCHANGE")
    # Запускаем задачу
    expensive_task = random.randint(0, 2)
    time.sleep(expensive_task)
    # logging.debug("Chanel %s", channel)
    # logging.debug("Method %s", method)
    # logging.debug("Properties %s", properties)
    # logging.info("Body %s", body)

    # Задержка для моделирования длительного процесса
    time.sleep(2)
    # if random.random() > 0.8:
    #
    #     logger.warning("ЗАПРОС НЕ БЫЛ УСПЕШНО ОБРАБОТАН И был возвращен в очередь (requeue=True).")
    #     channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    if random.random() > 0.5:

        logger.warning(f"ЗАПРОС {body} НЕ БЫЛ УСПЕШНО ОБРАБОТАН И  НЕ был возвращен в очередь (requeue=False).")
        # channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    else:

        logger.info(f"ЗАПРОС {body} БЫЛ УСПЕШНО ОБРАБОТАН.")
        channel.basic_ack(delivery_tag=method.delivery_tag)




def main() -> None:
    """
    Основная функция для настройки и запуска consumer для очереди сообщений RabbitMQ.
    """
    # Настройка логирования для RabbitMQ
    config_logging()

    # Инициализация клиента RabbitMQ с автоматическим закрытием соединения
    with MQDeadLetterExchangeLesson() as mq_with_dead_letter_ex:
        # Начинаем обработку сообщений
        mq_with_dead_letter_ex.declare_queue()
        mq_with_dead_letter_ex.consume_messages(
            on_message_callback=process_new_msg,  # Callback для обработки каждого сообщения
            queue_name="main-queue",  # Имя очереди для потребления
            exclusive=False,  # Если True, очередь привязывается только к этому consumer
        )


if __name__ == '__main__':
    # Устанавливаем уровень логирования и запускаем приложение
    try:
        logging.basicConfig(level=logging.INFO)
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)  # noqa
