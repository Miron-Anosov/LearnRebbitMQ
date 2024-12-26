import json
import logging
import time

from consumers_models.consumer_email_simple_dead_letter_exchange import MQDeadLetterExchangeLesson
from rabbitmq_conf import config_logging

logger = logging.getLogger(__name__)


class ProducerLessonDeadLetterExchange(MQDeadLetterExchangeLesson):

    def produce_message(
            self,
            exchange,
            routing_key,
            body,
            index: int
    ):
        """Producer."""
        message = {
            f"message-{index:02d}": body,
        }
        body_to_queue = json.dumps(message)
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=body_to_queue.encode(),
        )
        logger.info("Message sent to RabbitMQ : %s", body_to_queue)


def main() -> None:
    config_logging()
    with ProducerLessonDeadLetterExchange() as mq:
        for index in range(10):
            mq.produce_message(
                routing_key="main-queue",
                body="Hello world again!",
                index=index,
                exchange="main-exchange",
            )



if __name__ == '__main__':
    main()
