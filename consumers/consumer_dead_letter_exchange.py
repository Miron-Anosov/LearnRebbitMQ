import logging
import random
import time

from consumers_models.consumer_email_simple_dead_letter_exchange import RabbitMQWithDeadLetters

logger = logging.getLogger(__name__)


def process_main_message(channel, method, properties, body):
    """Обработчик для основной очереди"""
    try:
        # Имитация работы
        time.sleep(random.randint(1, 3))

        if random.random() > 0.5:
            raise Exception("Симуляция ошибки обработки")

        logger.info(f"Успешно обработано сообщение: {body}")
        channel.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logger.error(f"Ошибка обработки сообщения: {e}")
        # Отправляем в dead letter очередь
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def process_dead_letter(channel, method, properties, body):
    """Обработчик для dead letter очереди"""
    try:
        logger.warning(f"Обработка сообщения из dead letter очереди: {body}")
        # Здесь может быть логика для обработки "мертвых" сообщений
        # Например, сохранение в БД, отправка уведомления и т.д.

        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Ошибка обработки dead letter сообщения: {e}")
        # В случае ошибки можно решить, нужно ли пытаться еще раз
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    client = RabbitMQWithDeadLetters()

    with client:
        client.run(
            main_callback=process_main_message,
            dead_letter_callback=process_dead_letter
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()