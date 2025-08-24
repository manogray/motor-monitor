import aio_pika
import asyncio
import json


async def check_message_in_rabbitmq_queue(
        queue_name: str,
        rabbitmq_channel: aio_pika.Channel,
        expected_message: dict,
        timeout: int = 10
) -> bool:
    """
    Checks if a specific message is on queue

    :param queue_name: name of the queue
    :param rabbitmq_channel: reference to RabbitMQ channel
    :param expected_message: message expected to be received
    :param timeout: timeout to wait for the message
    :return: True if message was found, False otherwise
    """
    message_found = asyncio.Event()
    found_message = None

    async def message_handler(message: aio_pika.IncomingMessage):
        nonlocal found_message
        try:
            async with message.process():
                body = message.body.decode()
                received_message = json.loads(body)

                if received_message["data"] == expected_message["data"]:
                    found_message = received_message
                    message_found.set()
                    return

                await message.nack()

        except json.JSONDecodeError:
            await message.nack()
        except Exception as e:
            print(f"Error to process message: {e}")
            await message.nack()

    try:
        queue = await rabbitmq_channel.declare_queue(queue_name, passive=True)

        await queue.consume(message_handler)

        try:
            await asyncio.wait_for(message_found.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    except Exception as e:
        print(f"Not possible to process message: {e}")
        return False
