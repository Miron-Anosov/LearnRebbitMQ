import asyncio
import json


from aio_pika import Message

from asyncmq.worker import QueueRabbitClient


async def main():
    async with QueueRabbitClient(amqp_url="amqp://user:password@localhost/") as client:

        exchange = await client.declare_exchange("test_exchange", durable=True)
        queue = await client.declare_queue("test_queue", durable=True)
        await client.bind_queue(queue, exchange.name, routing_key="test_key")

        for i in range(10):
            message = {
                f"message-{i:02d}": "Hello World!",
            }
            body_to_queue = json.dumps(message)


            await exchange.publish(
                message=Message(body=body_to_queue.encode()),
                routing_key="test_key",
            )


if __name__ == '__main__':
    asyncio.run(main())