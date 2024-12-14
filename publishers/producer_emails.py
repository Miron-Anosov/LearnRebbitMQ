import json
import logging
import time

from consumers_models.consumer_email_update_kyc import EmailUpdateRabbit
from rabbitmq_conf import MQ_EMAIL_UPDATE_EXCHANGE_NAME

logger = logging.getLogger(__name__)


class ProducerEmails(EmailUpdateRabbit):

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
    with ProducerEmails() as mq:
        mq.declare_email_update_exchange()
        for index in range(10):
            mq.produce_message(
                routing_key="",
                body="Hello world again!",
                index=index,
                exchange=MQ_EMAIL_UPDATE_EXCHANGE_NAME,
            )
            time.sleep(2)


if __name__ == '__main__':
    main()
