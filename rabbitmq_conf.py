import pika
import logging

FORMAT_LOG_DEFAULT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

logger = logging.getLogger(__name__)

HOST = "0.0.0.0"
PORT = 5672
USER = "user"
PASSWORD = "password"

connection_params = pika.ConnectionParameters(
    host=HOST,
    port=PORT,
    credentials=pika.PlainCredentials(USER, PASSWORD)
)


def get_connection() -> pika.BlockingConnection:
    return pika.BlockingConnection(parameters=connection_params)


def config_logging(level: int = logging.INFO):
    logging.basicConfig(level=level,
                        format=FORMAT_LOG_DEFAULT,
                        datefmt='%Y-%m-%d %H:%M:%S')


MQ_EMAIL_UPDATE_EXCHANGE_NAME = "email_update_exchange"
MQ_EMAIL_NAME_UPDATE_QUEUE_KYC = "email_update_kyc"
MQ_EMAIL_NAME_UPDATE_NAW_LETTERS_QUEUE_KYC = "email_new_letters_update_kyc"
