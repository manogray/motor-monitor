import os
import asyncio
import json
import signal
import aio_pika
import datetime

from gmqtt import Client
from log_recorder import logger

STOP = asyncio.Event()


def ask_exit(*args) -> None:
    """
    Initiate graceful shutdown by setting the stop event flag.

    :param args: Ignored arguments for signal handler compatibility
    :return: None
    """
    STOP.set()


class MqttIngestor:
    """
    Class responsible to implements a microservice for ingesting MQTT messages from industrial sensors
    and forwarding them to RabbitMQ for further processing
    """
    def __init__(self):
        self.config = dict(
            MQTT_BROKER=os.getenv("MQTT_BROKER_REMOTE"),
            MQTT_PORT=int(os.getenv("MQTT_BROKER_PORT")),
            RABBITMQ_URL=os.getenv("RABBITMQ_URL"),
            EXCHANGE_NAME=os.getenv("EXCHANGE_NAME"),
            QUEUE_NAME=os.getenv("QUEUE_NAME")
        )
        self.mqtt_client = Client("mqtt-ingestor")
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_connect = self.on_connect
        self.rabbitmq_connection = None

    def on_connect(self, client, flags, rc, properties) -> None:
        """
        MQTT broker connect callback

        :param client: MQTT client instance
        :param flags: Connection flags
        :param rc: Return code indicating connection status
        :param properties: MQTT properties
        :return: None
        """
        logger.info("Connected to broker")
        client.subscribe("scgdi/motor/electrical", qos=1)
        client.subscribe("scgdi/motor/environment", qos=1)
        client.subscribe("scgdi/motor/vibration", qos=0)

    async def on_message(self, client, topic, payload, qos, properties) -> None:
        """
        MQTT broker message receiver callback

        :param client: MQTT client
        :param topic: MQTT topic the message was published
        :param payload: Raw message payload bytes
        :param qos: Quality of service level (0, 1, 2)
        :param properties: MQTT properties
        :return: None
        """
        try:
            data = json.loads(payload.decode())
            message = {
                "topic": topic,
                "data": data,
                "timestamp": datetime.datetime.now().isoformat()
            }

            if "electrical" in topic:
                routing_key = "motor.electrical"
            elif "environment" in topic:
                routing_key = "motor.environment"
            elif "vibration" in topic:
                routing_key = "motor.vibration"
            else:
                routing_key = "motor.data"

            success = await self.send_to_rabbitmq(message, routing_key)

            if success:
                logger.info("Message processed successfully")
            else:
                logger.error(f"Not possible to process message: {message}")

        except Exception as e:
            logger.error(f"Not possible to process MQTT message: {e}")

    async def send_to_rabbitmq(self, message: dict, routing_key: str) -> bool:
        """
        Publish validated MQTT message to RabbitMQ exchange

        :param message: Data that will be sent to RabbitMQ exchange
        :param routing_key: RabbitMQ routing key indicating data source
        :return: True if message was successfully published on RabbitMQ exchange
        """
        try:
            self.rabbitmq_connection = await aio_pika.connect_robust(self.config["RABBITMQ_URL"])
            async with self.rabbitmq_connection:
                channel = await self.rabbitmq_connection.channel()
                exchange = await channel.declare_exchange(
                    self.config["EXCHANGE_NAME"],
                    aio_pika.ExchangeType.TOPIC,
                    durable=True
                )
                queue = await channel.declare_queue(self.config["QUEUE_NAME"], durable=True)
                await queue.bind(exchange, routing_key)
                await exchange.publish(
                    aio_pika.Message(
                        body=json.dumps(message).encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                    ),
                    routing_key=routing_key
                )
                logger.info(f"Published {routing_key}: {message}")
                return True
        except Exception as e:
            logger.error(f"Not possible to send data to RabbitMQ: {e}")
            return False

    async def connect_to_broker(self, attempts: int = 5) -> None:
        """
        Connect the MQTT client to broker with retry logic

        :param attempts: Number of attempts that client will try to connect to broker
        :return: None
        """
        for attempt in range(attempts):
            try:
                await self.mqtt_client.connect(self.config["MQTT_BROKER"], self.config["MQTT_PORT"])
                return
            except Exception as e:
                logger.warning(f"Trying to connect to {self.config['MQTT_BROKER']}:{self.config['MQTT_PORT']} - "
                               f"attempt [{attempt+1}/{attempts}]")
                logger.warning(f"Failed: {e}")
                await asyncio.sleep(3)
        raise RuntimeError("Not possible to connect to MQTT broker")


async def main():
    """
    Main function to be executed on this microservice

    :return:
    """
    ingestor = MqttIngestor()

    await ingestor.connect_to_broker()

    await STOP.wait()
    await ingestor.mqtt_client.disconnect()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    loop.add_signal_handler(signal.SIGINT, ask_exit)
    loop.add_signal_handler(signal.SIGTERM, ask_exit)

    loop.run_until_complete(main())
