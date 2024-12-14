import pika
import logging

logger = logging.getLogger(__name__)

HOST = "0.0.0.0"
PORT = 5672
USER = "user"
PASSWORD = "password"

class RabbitRuntimeException(RuntimeError):
    """
    Исключение, которое выбрасывается при ошибках, связанных с работой RabbitMQ.
    Например, если канал не инициализирован.
    """
    pass

# Параметры подключения к RabbitMQ-серверу
mq_connection_params = pika.ConnectionParameters(
    host=HOST,  # Адрес хоста RabbitMQ
    port=PORT,  # Порт для подключения
    credentials=pika.PlainCredentials(USER, PASSWORD)  # Учетные данные пользователя
)

class RabbitMQClientBase:
    """
    Базовый класс для работы с RabbitMQ, предоставляющий основные методы для управления соединением и каналом.

    Атрибуты:
        connection_params (pika.ConnectionParameters): Параметры подключения к RabbitMQ.
        _connection (pika.BlockingConnection | None): Активное соединение с RabbitMQ.
        _channel (pika.adapters.blocking_connection.BlockingChannel | None): Канал для взаимодействия с RabbitMQ.
    """

    def __init__(self,
                 connection_params: pika.ConnectionParameters = mq_connection_params
                 ) -> None:
        """
        Инициализация клиента RabbitMQ.

        Аргументы:
            connection_params (pika.ConnectionParameters): Параметры подключения к RabbitMQ.
        """
        self.connection_params: pika.ConnectionParameters = connection_params
        self._connection: pika.BlockingConnection | None = None  # Активное соединение
        self._channel: pika.adapters.blocking_connection.BlockingChannel | None = None  # Канал связи

    def get_connection(self) -> pika.BlockingConnection:
        """
        Создает новое соединение с RabbitMQ.

        Возвращает:
            pika.BlockingConnection: Объект соединения с RabbitMQ.
        """
        return pika.BlockingConnection(parameters=self.connection_params)

    @property
    def channel(self) -> pika.adapters.blocking_connection.BlockingChannel:
        """
        Возвращает активный канал связи с RabbitMQ.

        Если канал не был инициализирован, выбрасывается исключение RabbitRuntimeException.

        Возвращает:
            pika.adapters.blocking_connection.BlockingChannel: Активный канал связи.

        Исключения:
            RabbitRuntimeException: Если канал не инициализирован.
        """
        if self._channel is None:
            raise RabbitRuntimeException("Channel is not yet initialized")
        return self._channel

    def __enter__(self):
        """
        Контекстный менеджер: инициализирует соединение и канал при входе в контекст.

        Возвращает:
            self: Текущий экземпляр класса с активным соединением и каналом.
        """
        self._connection = self.get_connection()  # Создаем соединение с RabbitMQ
        self._channel = self._connection.channel()  # Открываем канал связи
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Контекстный менеджер: закрывает соединение и канал при выходе из контекста.

        Аргументы:
            exc_type: Тип исключения (если возникло).
            exc_val: Значение исключения (если возникло).
            exc_tb: Трейсбек исключения (если возникло).
        """
        # Закрываем канал, если он открыт
        if self._channel and self._channel.is_open:
            self._channel.close()
        # Закрываем соединение, если оно открыто
        if self._connection and self._connection.is_open:
            self._connection.close()
