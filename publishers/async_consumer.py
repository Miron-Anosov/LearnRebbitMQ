"""Consumer NewUSer."""
import aio_pika
from aio_pika.abc import AbstractQueue, AbstractChannel
from src.core.infrastructure.messaging.clients.consumer_async_base import AsyncRabbitMQClient



class CreateUserRabbitMixin:
    """
    Класс-миксин для работы с RabbitMQ.

    Содержит вспомогательные методы для объявления очередей, обменников,
    привязки очередей к обменникам и обработки сообщений.
    """
    channel: "AbstractChannel"

    async def declare_exchange(self, exchange_name: str):
        """
        Объявляет обменник с типом FANOUT.

        :param exchange_name: Имя обменника.
        """
        await self.channel.declare_exchange(
            exchange_name, aio_pika.ExchangeType.FANOUT
        )

    async def declare_queue(self, queue_name: str = "", exclusive: bool = True) -> AbstractQueue:
        """
        Объявляет очередь.

        :param queue_name: Имя очереди (по умолчанию пустая строка).
        :param exclusive: Флаг эксклюзивной очереди (по умолчанию True).
        :return: Объект AbstractQueue.
        """
        # Убеждаемся, что exchange существует (идемпотентная операция).
        await self.declare_exchange(queue_name)
        # Объявляем очередь с заданными параметрами.
        queue = await self.channel.declare_queue(queue_name, exclusive=exclusive)
        return queue

    async def bind_queue(self, queue: AbstractQueue, exchange_name: str):
        """
        Привязывает очередь к обменнику.

        :param queue: Объект AbstractQueue.
        :param exchange_name: Имя обменника.
        """
        exchange = await self.channel.get_exchange(exchange_name)
        await queue.bind(exchange)


    @staticmethod
    async def consume(queue: AbstractQueue, callback):
        """
        Начинает обработку сообщений из очереди.

        :param queue: Объект AbstractQueue.
        :param callback: Функция обратного вызова для обработки сообщений.
        """

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    await callback(message)



class UserCreateAsyncRabbit(CreateUserRabbitMixin, AsyncRabbitMQClient):
    """
    Клиент RabbitMQ для обработки сообщений о создании пользователей.

    Комбинирует базовый функционал RabbitMQ из AsyncRabbitMQClient с методами,
    предоставляемыми CreateUserRabbitMixin.
    """
    pass
