"""Asynchronous consumer client implementation."""
from typing import Optional

import aio_pika
from aio_pika.abc import AbstractChannel
import logging

logger = logging.getLogger(__name__)

HOST = "0.0.0.0"
PORT = 5672
USER = "user"
PASSWORD = "password"


class RabbitMQClient:
    def __init__(self, amqp_url: str):
        self.amqp_url = amqp_url
        self.connection: aio_pika.RobustConnection | None = None
        self.channel: Optional["AbstractChannel"] = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.amqp_url)
        self.channel = await self.connection.channel()

    async def disconnect(self):
        if self.channel:
            await self.channel.close()
        if self.connection:
            await self.connection.close()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()
