import logging
from typing import TYPE_CHECKING, Callable

from pika.exchange_type import ExchangeType
from pika.spec import Basic, BasicProperties



from consumers_models.consumer_base import RabbitMQClientBase
from rabbitmq_conf import MQ_EMAIL_UPDATE_EXCHANGE_NAME

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel

logger = logging.getLogger(__name__)


class EmailUpdateRabbitMixin:
    """
    Класс-миксин, предоставляющий функциональность RabbitMQ для обработки сообщений об обновлении email.

    Содержит методы для объявления exchange, очередей и обработки сообщений, связанных с обновлением email.
    """

    channel: "BlockingChannel"

    def declare_email_update_exchange(self) -> None:
        """
        Объявляет exchange, используемый для обновлений email.

        Exchange имеет тип `fanout`, который рассылает сообщения всем связанным очередям.
        """
        self.channel.exchange_declare(
            exchange=MQ_EMAIL_UPDATE_EXCHANGE_NAME,
            exchange_type=ExchangeType.fanout,
        )

    def declare_queue_email_updates(
            self, queue_name: str = "",
            exclusive: bool = True
    ) -> str | None:
        """
        Объявляет очередь для получения обновлений email и связывает её с exchange.

        Аргументы:
            queue_name (str): Имя очереди для объявления. Если не указано, будет сгенерировано уникальное имя.
            exclusive (bool): Если True, очередь будет эксклюзивной для соединения и удалена при его закрытии.

        Возвращает:
            str | None: Имя объявленной очереди.
        """
        # Убеждаемся, что exchange существует (идемпотентная операция).
        self.declare_email_update_exchange()

        # Объявляем очередь с заданными параметрами.
        queue = self.channel.queue_declare(queue=queue_name, exclusive=exclusive)
        q_name = queue.method.queue

        # Связываем объявленную очередь с exchange для получения сообщений.
        self.channel.queue_bind(
            exchange=MQ_EMAIL_UPDATE_EXCHANGE_NAME,
            queue=queue_name,
        )

        return q_name

    def consume_messages(
            self,
            on_message_callback: Callable[
                ["BlockingChannel", "Basic.Deliver", "BasicProperties", bytes], None
            ],
            exclusive: bool = True,
            prefetch_count: int = 1,
            auto_ack: bool = False,
            queue_name: str = ""
    ) -> None:
        """
        Настраивает consumer для обработки сообщений из очереди обновлений email.

        Аргументы:
            on_message_callback (Callable): Callback-функция для обработки входящих сообщений.
            prefetch_count (int): Максимальное количество непотверждённых сообщений, которые может получить consumer.
            auto_ack (bool): Если True, сообщения автоматически подтверждаются при получении.
        """
        # Устанавливаем максимальное количество необработанных сообщений, которое может принять consumer.
        self.channel.basic_qos(prefetch_count=prefetch_count)

        # Объявляем очередь и связываем её с exchange.
        queue_name = self.declare_queue_email_updates(queue_name=queue_name, exclusive=exclusive)  # type: ignore

        # Настраиваем consumer для прослушивания сообщений в очереди.
        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=on_message_callback,
            auto_ack=auto_ack,
        )

        logger.info("Ожидание сообщений в очереди: %s", queue_name)

        # Запускаем цикл обработки сообщений.
        self.channel.start_consuming()


class EmailUpdateRabbit(EmailUpdateRabbitMixin, RabbitMQClientBase):
    """
    Клиент RabbitMQ для обработки сообщений об обновлении email.

    Комбинирует базовый функционал RabbitMQ из `RabbitMQClientBase` с
    email-специфическими функциями, предоставляемыми `EmailUpdateRabbitMixin`.
    """
    pass

