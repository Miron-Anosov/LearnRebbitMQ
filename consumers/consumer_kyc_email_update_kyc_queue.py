import logging
import sys
import os
import time

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from consumers_models.consumer_email_update_kyc import EmailUpdateRabbit
from rabbitmq_conf import MQ_EMAIL_NAME_UPDATE_QUEUE_KYC, config_logging

logger = logging.getLogger(__name__)


def process_new_msg(
        channel: "BlockingChannel",
        method: "Basic.Deliver",
        properties: "BasicProperties",
        body: bytes,
):
    logging.debug("Канал %s", channel)
    logging.debug("Метод %s", method)
    logging.debug("Свойства %s", properties)
    logging.info("Тело %s", body)
    time.sleep(4)  # задержка для отладки: какой-то долгий процесс.

    channel.basic_ack(delivery_tag=method.delivery_tag)  # type: ignore # подтверждаем выполнение задачи.
    logger.warning("Завершена работа")


def main() -> None:
    config_logging()
    with EmailUpdateRabbit() as mq_email:
        mq_email.consume_messages(
            on_message_callback=process_new_msg,
            queue_name=MQ_EMAIL_NAME_UPDATE_QUEUE_KYC,
            exclusive=False,
            # привязывается только к одному подключению и будет автоматически удалена, когда это подключение закроется.
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
