import logging
from typing import TYPE_CHECKING, Callable, Optional

from pika.exchange_type import ExchangeType
from pika.spec import Basic, BasicProperties

from consumers_models.consumer_base import RabbitMQClientBase


if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel

logger = logging.getLogger(__name__)


class SimpleRabbitMixin:
    """
    Класс-миксин, предоставляющий функциональность RabbitMQ для обработки сообщений об обновлении email.

    Содержит методы для объявления exchange, очередей и обработки сообщений, связанных с обновлением email.
    """

    channel: "BlockingChannel"

    def declare_exchange(self) -> None:
        """
        Объявляет exchange, используемый для обновлений email.

        Exchange имеет тип `fanout`, который рассылает сообщения всем связанным очередям.
        """
        self.channel.exchange_declare(
            exchange="main-exchange",
            exchange_type=ExchangeType.fanout,
        )

    def declare_queue(
            self, queue_name: str = "dead-letter-queue",
            passive=False,
            durable=False,
            exclusive=False,
            auto_delete=False,
            arguments={"x-dead-letter-exchange": "dead-letter-exchange",  # выбираем обменник куда будут перенапрвляться неудачные сообщения
                       "x-dead-letter-routing-key": "dead-letter-queue"},  # выбираем в какую очередь отправлять неудачные сообщения
    ) -> str:
        """
        Объявляет очередь для получения обновлений email и связывает её с exchange.

        Аргументы:
            queue_name (str): Имя очереди для объявления. Если не указано, будет сгенерировано уникальное имя.
            exclusive (bool): Если True, очередь будет эксклюзивной для соединения и удалена при его закрытии.

        Возвращает:
            str: Имя объявленной очереди.
        """
        # Убеждаемся, что exchange существует (идемпотентная операция).
        self.declare_exchange()

        # Объявляем очередь с заданными параметрами.
        queue = self.channel.queue_declare(queue=queue_name,
                                           passive=passive,
                                           durable=durable,
                                           exclusive=exclusive,
                                           auto_delete=auto_delete,
                                           arguments=arguments)
        q_name = queue.method.queue

        # Связываем объявленную очередь с exchange для получения сообщений.
        self.channel.queue_bind(
            exchange="dead-letter-exchange",
            queue=q_name,  # Здесь передаём имя объявленной очереди
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
        queue_name = self.declare_queue(queue_name=queue_name, exclusive=exclusive)  # type: ignore

        # Настраиваем consumer для прослушивания сообщений в очереди.
        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=on_message_callback,
            auto_ack=auto_ack,
        )

        logger.info("Ожидание сообщений в очереди: %s", queue_name)

        # Запускаем цикл обработки сообщений.
        self.channel.start_consuming()


class MQDeadLetterExchangeLesson(SimpleRabbitMixin, RabbitMQClientBase):
    """
    Клиент RabbitMQ для обработки сообщений об обновлении email.

    Комбинирует базовый функционал RabbitMQ из `RabbitMQClientBase` с
    email-специфическими функциями, предоставляемыми `EmailUpdateRabbitMixin`.
    """
    pass


class RabbitMQWithDeadLetters(RabbitMQClientBase):
    """
    Реализация RabbitMQ клиента с поддержкой Dead Letter Exchange
    """
    def __init__(self):
        super().__init__()
        # Имена для основного обменника и очереди
        self.main_exchange = "main-exchange"
        self.main_queue = "main-queue"
        # Имена для dead letter обменника и очереди
        self.dead_letter_exchange = "dead-letter-exchange"
        self.dead_letter_queue = "dead-letter-queue"

    def setup_exchanges(self) -> None:
        """Объявление основного и dead letter обменников"""
        # Объявляем dead letter exchange
        self.channel.exchange_declare(
            exchange=self.dead_letter_exchange,
            exchange_type=ExchangeType.fanout,
            durable=True
        )

        # Объявляем основной exchange
        self.channel.exchange_declare(
            exchange=self.main_exchange,
            exchange_type=ExchangeType.fanout,
            durable=True
        )

    def setup_queues(self) -> None:
        """Объявление основной и dead letter очередей"""
        # Объявляем dead letter очередь
        self.channel.queue_declare(
            queue=self.dead_letter_queue,
            durable=True,
            arguments={}  # Здесь не нужны специальные аргументы
        )

        # Привязываем dead letter очередь к dead letter exchange
        self.channel.queue_bind(
            exchange=self.dead_letter_exchange,
            queue=self.dead_letter_queue
        )

        # Объявляем основную очередь с привязкой к dead letter exchange
        self.channel.queue_declare(
            queue=self.main_queue,
            durable=True,
            arguments={
                "x-dead-letter-exchange": self.dead_letter_exchange,
                "x-dead-letter-routing-key": self.dead_letter_queue
            }
        )

        # Привязываем основную очередь к основному exchange
        self.channel.queue_bind(
            exchange=self.main_exchange,
            queue=self.main_queue
        )

    def setup_dead_letter_consumer(self, callback) -> None:
        """Настройка consumer'а для dead letter очереди"""
        self.channel.basic_consume(
            queue=self.dead_letter_queue,
            on_message_callback=callback,
            auto_ack=False
        )

    def setup_main_consumer(self, callback) -> None:
        """Настройка consumer'а для основной очереди"""
        self.channel.basic_consume(
            queue=self.main_queue,
            on_message_callback=callback,
            auto_ack=False
        )

    def run(self, main_callback, dead_letter_callback: Optional[callable] = None) -> None:
        """Запуск обработки сообщений"""
        # Настраиваем инфраструктуру
        self.setup_exchanges()
        self.setup_queues()

        # Устанавливаем основной consumer
        self.setup_main_consumer(main_callback)

        # Если предоставлен callback для dead letter очереди, устанавливаем его
        if dead_letter_callback:
            self.setup_dead_letter_consumer(dead_letter_callback)

        logger.info("Начинаем прослушивание очередей")
        self.channel.start_consuming()