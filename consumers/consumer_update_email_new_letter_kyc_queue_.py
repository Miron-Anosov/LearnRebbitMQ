import logging
import sys
import os
import time

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from consumers_models.consumer_email_update_kyc import EmailUpdateRabbit
from rabbitmq_conf import config_logging, MQ_EMAIL_NAME_UPDATE_NAW_LETTERS_QUEUE_KYC

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
    logging.debug("Канал: %s", channel)
    logging.debug("Метод: %s", method)
    logging.debug("Свойства: %s", properties)
    logging.info("Тело сообщения: %s", body)

    # Задержка для моделирования длительного процесса
    time.sleep(2)

    # Подтверждаем обработку сообщения
    channel.basic_ack(delivery_tag=method.delivery_tag)  # type: ignore
    logger.warning("Завершена работа с сообщением.")


def main() -> None:
    """
    Основная функция для настройки и запуска consumer для очереди сообщений RabbitMQ.
    """
    # Настройка логирования для RabbitMQ
    config_logging()

    # Инициализация клиента RabbitMQ с автоматическим закрытием соединения
    with EmailUpdateRabbit() as mq_email:
        # Начинаем обработку сообщений
        mq_email.consume_messages(
            on_message_callback=process_new_msg,  # Callback для обработки каждого сообщения
            queue_name=MQ_EMAIL_NAME_UPDATE_NAW_LETTERS_QUEUE_KYC,  # Имя очереди для потребления
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
