"""
Класс QueueRabbitClient предоставляет высокоуровневый интерфейс для работы с RabbitMQ через aio_pika.

Он включает методы для объявления обменников, очередей, привязки очередей к обменникам,
и обработки сообщений. Подходит для создания потребителей и отправителей сообщений.
"""
import asyncio
import logging

from typing import Union, Awaitable, Any, Callable, Optional

from aio_pika.abc import (
    AbstractQueue, AbstractChannel, ExchangeType, TimeoutType, AbstractExchange, AbstractIncomingMessage, ConsumerTag)
from pamqp.common import Arguments

from asyncmq.connection import RabbitMQClient

logger = logging.getLogger(__name__)

class QueueRabbitClient(RabbitMQClient):
    """
    Класс для работы с RabbitMQ.

    Содержит вспомогательные методы для объявления очередей, обменников,
    привязки очередей к обменникам и обработки сообщений.
    """
    channel: "AbstractChannel"

    async def declare_exchange(
            self,
            exchange: str,
            exchange_type: Union[ExchangeType, str] = ExchangeType.FANOUT,
            *,
            durable: bool = False,
            auto_delete: bool = False,
            internal: bool = False,
            passive: bool = False,
            arguments: Arguments = None,
            timeout: TimeoutType = None,
    ) -> AbstractExchange:
        """
        Объявляет обменник.

        :param exchange: Имя обменника.
        :param exchange_type: Тип обменника (DIRECT, FANOUT, TOPIC, или HEADERS).
        :param durable: Указывает, сохраняется ли обменник после перезапуска RabbitMQ (по умолчанию False).
        :param auto_delete: Указывает, удаляется ли обменник автоматически при отсутствии подключений (по умолчанию False).
        :param internal: Определяет, является ли обменник внутренним (по умолчанию False).
        :param passive: Если True, проверяет существование обменника, но не создает его (по умолчанию False).
        :param arguments: Дополнительные аргументы для конфигурации обменника.
        :param timeout: Максимальное время ожидания объявления обменника.
        :return: AbstractExchange - объект обменника.
        """
        return await self.channel.declare_exchange(
            name=exchange,
            type=exchange_type,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal,
            passive=passive,
            arguments=arguments,
            timeout=timeout,
        )

    async def declare_queue(
            self,
            queue,
            passive=False,
            durable=False,
            auto_delete=False,
            arguments=None,
            exclusive: bool = False,

    ) -> AbstractQueue:
        """
        Объявляет очередь.

        :param queue: Имя очереди.
        :param passive: Если True, проверяет существование очереди, но не создает ее (по умолчанию False).
        :param durable: Указывает, сохраняется ли очередь после перезапуска RabbitMQ (по умолчанию False).
        :param auto_delete: Указывает, удаляется ли очередь автоматически при отсутствии подключений (по умолчанию False).
        :param arguments: Дополнительные аргументы для конфигурации очереди.
        :param exclusive: Указывает, является ли очередь доступной только для текущего соединения (по умолчанию False).
        :return: AbstractQueue - объект очереди.
        """
        # Объявляем очередь с заданными параметрами.
        return await self.channel.declare_queue(
            name=queue,
            durable=durable,
            exclusive=exclusive,
            passive=passive,
            arguments=arguments,
            auto_delete=auto_delete,

        )

    async def bind_queue(
            self,
            queue: AbstractQueue,
            exchange_name: str,
            routing_key: Optional[str] = None,
            *,
            arguments: Arguments = None,
            timeout: TimeoutType = None,

    ):
        """
        Привязывает очередь к обменнику.

        :param queue: Объект AbstractQueue, который будет привязан.
        :param exchange_name: Имя обменника.
        :param routing_key: Ключ маршрутизации, используемый для привязки (по умолчанию None).
        :param arguments: Дополнительные аргументы для конфигурации привязки.
        :param timeout: Максимальное время ожидания операции привязки.
        """
        exchange = await self.channel.get_exchange(exchange_name)
        await queue.bind(
            exchange=exchange,
            routing_key=routing_key,
            arguments=arguments,
            timeout=timeout,
        )

    @staticmethod
    async def consume(
            queue: AbstractQueue,
            on_message_callback: Callable[[AbstractIncomingMessage], Awaitable[Any]],
            auto_ack: bool = False,
            exclusive: bool = False,
            arguments: Arguments = None,
            consumer_tag: Optional[ConsumerTag] = None,
            timeout: TimeoutType = None,
    ):
        """
        Начинает обработку сообщений из очереди.

        :param queue: Объект AbstractQueue, из которого будут потребляться сообщения.
        :param on_message_callback: Асинхронная функция обратного вызова для обработки сообщений.
        :param auto_ack: Указывает, нужно ли автоматически подтверждать сообщения (по умолчанию False).
        :param exclusive: Указывает, эксклюзивен ли потребитель для этой очереди (по умолчанию False).
        :param arguments: Дополнительные аргументы для конфигурации потребителя.
        :param consumer_tag: Уникальный идентификатор потребителя (по умолчанию None).
        :param timeout: Максимальное время ожидания для регистрации потребителя.
        """
        # Создаём итератор очереди с переданными параметрами
        message: AbstractIncomingMessage
        try:
            async with queue.iterator(
                    no_ack=auto_ack,
                    exclusive=exclusive,
                    arguments=arguments,
                    consumer_tag=consumer_tag,
                    timeout=timeout
            ) as queue_iter:
                async for message in queue_iter:
                    try:
                        await on_message_callback(message)
                    except Exception as e:
                        logger.exception(f"Error processing message: {e}")

        except Exception as e:
            logger.exception(e)
            raise e


class DeadLetterQueueClient(QueueRabbitClient):
    def __init__(self, amqp_url: str):
        super().__init__(amqp_url=amqp_url)
        # Имена для основного обменника и очереди
        self.main_exchange = "main-exchange"
        self.main_queue = "main-queue"
        # Имена для dead letter обменника и очереди
        self.dead_letter_exchange = "dead-letter-exchange"
        self.dead_letter_queue = "dead-letter-queue"

    async def setup_infrastructure(self):
        """Настройка всей инфраструктуры очередей и обменников"""

        # 1. Создаем dead letter exchange
        await self.declare_exchange(
            exchange=self.dead_letter_exchange,
            exchange_type=ExchangeType.FANOUT,
            durable=True
        )

        # 2. Создаем dead letter очередь
        dead_letter_queue = await self.declare_queue(
            queue=self.dead_letter_queue,
            durable=True
        )

        # 3. Привязываем dead letter очередь к dead letter exchange
        await self.bind_queue(
            queue=dead_letter_queue,
            exchange_name=self.dead_letter_exchange
        )

        # 4. Создаем основной exchange
        await self.declare_exchange(
            exchange=self.main_exchange,
            exchange_type=ExchangeType.FANOUT,
            durable=True
        )

        # 5. Создаем основную очередь с настройками DLX
        main_queue = await self.declare_queue(
            queue=self.main_queue,
            durable=True,
            arguments={
                "x-dead-letter-exchange": self.dead_letter_exchange,
                "x-dead-letter-routing-key": self.dead_letter_queue
            }
        )

        # 6. Привязываем основную очередь к основному exchange
        await self.bind_queue(
            queue=main_queue,
            exchange_name=self.main_exchange
        )

        return main_queue, dead_letter_queue

    async def run(self, main_callback, dead_letter_callback):
        """Запуск обработки сообщений"""
        main_queue, dead_letter_queue = await self.setup_infrastructure()

        # Запускаем обработчики для обеих очередей
        await asyncio.gather(
            # self.consume(main_queue, main_callback),
            self.consume(dead_letter_queue, dead_letter_callback)
        )