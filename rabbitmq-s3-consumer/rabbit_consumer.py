import pika
import json
import sys
import signal
import logging
import boto3
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RabbitMQConsumer:
    def __init__(self):
        self.host = os.getenv("RABBITMQ_HOST")
        self.port = int(os.getenv("RABBITMQ_PORT"))
        self.username = os.getenv("RABBITMQ_USERNAME")
        self.password = os.getenv("RABBITMQ_PASSWORD")
        self.queue_name = os.getenv("RABBITMQ_QUEUE_NAME")
        self.vhost = os.getenv(
            "RABBITMQ_VHOST", "/"
        )  # Added vhost with default value "/"
        self.connection = None
        self.channel = None
        self._setup_connection()

        # Initialize MinIO client
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=os.getenv("MINIO_ENDPOINT"),
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
            config=boto3.session.Config(signature_version="s3v4"),
            region_name="us-east-1",  # This can be any region for MinIO
        )
        self.bucket_name = os.getenv("MINIO_BUCKET_NAME")
        self.folder_path = os.getenv("MINIO_FOLDER_PATH", "")

    def _setup_connection(self):
        """Establish connection to RabbitMQ"""
        try:
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                credentials=credentials,
                virtual_host=self.vhost,  # Added vhost parameter
            )
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            logger.info(f"Connected to RabbitMQ and declared queue: {self.queue_name}")
        except Exception as e:
            logger.error(f"Error connecting to RabbitMQ: {e}")
            sys.exit(1)

    def upload_to_minio(self, data):
        """Upload JSON data to MinIO"""
        try:
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            key = f"{self.folder_path}{timestamp}.json"

            # Upload to MinIO
            self.s3_client.put_object(
                Bucket=self.bucket_name, Key=key, Body=json.dumps(data)
            )
            logger.info(f"Successfully uploaded to MinIO: {key}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload to MinIO: {e}")
            return False

    def callback(self, ch, method, properties, body):
        """Process received messages"""
        try:
            message = json.loads(body)
            logger.info(f"Received message: {message}")

            # Upload to MinIO
            if self.upload_to_minio(message):
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                # If upload fails, requeue the message
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {e}")
            # Reject the message
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            # Reject and requeue the message
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def start_consuming(self):
        """Start consuming messages"""
        try:
            # Set up quality of service
            self.channel.basic_qos(prefetch_count=1)

            # Set up consumer
            self.channel.basic_consume(
                queue=self.queue_name, on_message_callback=self.callback
            )

            logger.info("Started consuming messages. To exit press CTRL+C")
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
            self.stop()

    def stop(self):
        """Close the connection"""
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("Connection closed")


def main():
    consumer = RabbitMQConsumer()

    # Setup signal handlers
    def signal_handler(sig, frame):
        logger.info("Caught signal, stopping consumer...")
        consumer.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start consuming
    consumer.start_consuming()


if __name__ == "__main__":
    main()
