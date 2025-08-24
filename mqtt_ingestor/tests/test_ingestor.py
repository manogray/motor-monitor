import pytest
import asyncio
import json

from datetime import datetime
from mqtt_ingestor.tests.utils import check_message_in_rabbitmq_queue


@pytest.mark.asyncio
async def test_connect_to_broker(get_ingestor_instance, test_log):
    try:
        ingestor = get_ingestor_instance

        test_log.info("Connecting to MQTT broker")
        await ingestor.connect_to_broker()

        await asyncio.sleep(3)

        test_log.info("Disconnecting to MQTT broker")
        await ingestor.mqtt_client.disconnect()
    except Exception as e:
        raise pytest.fail(f"Not possible to connect to broker: {e}")


@pytest.mark.asyncio
async def test_send_to_rabbitmq_electrical(use_ingestor_connected, get_rabbitmq_config, use_rabbitmq_client, test_log):
    try:
        ingestor = use_ingestor_connected
        _, rabbitmq_queue_name = get_rabbitmq_config
        rabbitmq_connection, rabbitmq_channel = use_rabbitmq_client
        routing_key = "motor.electrical"

        expected_message = {
            "timestamp": datetime.now().isoformat(),
            "topic": "scgdi/motor/electrical",
            "data": {
                "timestamp": datetime.now().isoformat(),
                "voltage": {"a": 220.0, "b": 219.8, "c": 220.5},
                "current": {"a": 10.3, "b": 10.5, "c": 10.5},
                "power": {"active": 4502, "reactive": 494, "apparent": 4605},
                "energy": {"active": 10050, "reactive": 1204, "apparent": 10201},
                "powerFactor": 0.94,
                "frequency": 60.0
            }
        }

        test_log.info(f"Sending message to RabbitMQ: routing_key={routing_key}")
        success = await ingestor.send_to_rabbitmq(expected_message, routing_key)

        if not success:
            pytest.fail(f"Not possible to send messages to RabbitMQ: ({routing_key}): {expected_message}")

        test_log.info("Checking if RabbitMQ receive the message")
        queue = await rabbitmq_channel.declare_queue(rabbitmq_queue_name, passive=True)
        message_count = queue.declaration_result.message_count
        test_log.info(f"Queue {rabbitmq_queue_name} have {message_count} new messages")

        if message_count > 0:
            message_found = await check_message_in_rabbitmq_queue(
                rabbitmq_queue_name,
                rabbitmq_channel,
                expected_message
            )

            if not message_found:
                pytest.fail("Message was not found on RabbitMQ")
        else:
            pytest.fail("No messages received on RabbitMQ")

    except Exception as e:
        pytest.fail(f"Failed to send message to RabbitMQ using Ingestor method: {e}")


@pytest.mark.asyncio
async def test_send_to_rabbitmq_environment(use_ingestor_connected, get_rabbitmq_config, use_rabbitmq_client, test_log):
    try:
        ingestor = use_ingestor_connected
        _, rabbitmq_queue_name = get_rabbitmq_config
        rabbitmq_connection, rabbitmq_channel = use_rabbitmq_client
        routing_key = "motor.environment"

        expected_message = {
            "timestamp": datetime.now().isoformat(),
            "topic": "scgdi/motor/environment",
            "data": {
                "timestamp": datetime.now().isoformat(),
                "temperature": 36.6,
                "humidity": 58.2,
                "caseTemperature": 60.4
            }
        }

        test_log.info(f"Sending message to RabbitMQ: routing_key={routing_key}")
        success = await ingestor.send_to_rabbitmq(expected_message, routing_key)

        if not success:
            pytest.fail(f"Not possible to send messages to RabbitMQ: ({routing_key}): {expected_message}")

        test_log.info("Checking if RabbitMQ receive the message")
        queue = await rabbitmq_channel.declare_queue(rabbitmq_queue_name, passive=True)
        message_count = queue.declaration_result.message_count
        test_log.info(f"Queue {rabbitmq_queue_name} have {message_count} new messages")

        if message_count > 0:
            message_found = await check_message_in_rabbitmq_queue(
                rabbitmq_queue_name,
                rabbitmq_channel,
                expected_message
            )

            if not message_found:
                pytest.fail("Message was not found on RabbitMQ")
        else:
            pytest.fail("No messages received on RabbitMQ")

    except Exception as e:
        pytest.fail(f"Failed to send message to RabbitMQ using Ingestor method: {e}")


@pytest.mark.asyncio
async def test_send_to_rabbitmq_vibration(use_ingestor_connected, get_rabbitmq_config, use_rabbitmq_client, test_log):
    try:
        ingestor = use_ingestor_connected
        _, rabbitmq_queue_name = get_rabbitmq_config
        rabbitmq_connection, rabbitmq_channel = use_rabbitmq_client
        routing_key = "motor.vibration"

        expected_message = {
            "timestamp": datetime.now().isoformat(),
            "topic": "scgdi/motor/vibration",
            "data": {
                "timestamp": datetime.now().isoformat(),
                "axial": 0.14,
                "radial": 0.12
            }
        }

        test_log.info(f"Sending message to RabbitMQ: routing_key={routing_key}")
        success = await ingestor.send_to_rabbitmq(expected_message, routing_key)

        if not success:
            pytest.fail(f"Not possible to send messages to RabbitMQ: ({routing_key}): {expected_message}")

        test_log.info("Checking if RabbitMQ receive the message")
        queue = await rabbitmq_channel.declare_queue(rabbitmq_queue_name, passive=True)
        message_count = queue.declaration_result.message_count
        test_log.info(f"Queue {rabbitmq_queue_name} have {message_count} new messages")

        if message_count > 0:
            message_found = await check_message_in_rabbitmq_queue(
                rabbitmq_queue_name,
                rabbitmq_channel,
                expected_message
            )

            if not message_found:
                pytest.fail("Message was not found on RabbitMQ")
        else:
            pytest.fail("No messages received on RabbitMQ")

    except Exception as e:
        pytest.fail(f"Failed to send message to RabbitMQ using Ingestor method: {e}")


@pytest.mark.asyncio
async def test_on_message_electrical(
        gmqtt_client,
        use_ingestor_connected,
        get_rabbitmq_config,
        use_rabbitmq_client,
        test_log
):

    test_log.info("MQTT Ingestor started")
    ingestor = use_ingestor_connected

    _, rabbitmq_queue_name = get_rabbitmq_config
    rabbitmq_connection, rabbitmq_channel = use_rabbitmq_client

    topic = "scgdi/motor/electrical"
    payload = {
        "timestamp": datetime.now().isoformat(),
        "voltage": {"a": 220.0, "b": 221.1, "c": 219.5},
        "current": {"a": 10.5, "b": 10.4, "c": 10.6},
        "power": {"active": 4500, "reactive": 500, "apparent": 4600},
        "energy": {"active": 10000, "reactive": 1200, "apparent": 10200},
        "powerFactor": 0.95,
        "frequency": 60.0
    }

    expected_message = {
        "timestamp": datetime.now().isoformat(),
        "topic": topic,
        "data": payload
    }

    try:
        test_log.info("Publishing on MQTT electrical motor information")
        gmqtt_client.publish(topic, json.dumps(payload))
        await asyncio.sleep(0.5)

        test_log.info("Checking if RabbitMQ receive new messages")
        queue = await rabbitmq_channel.declare_queue(rabbitmq_queue_name, passive=True)
        message_count = queue.declaration_result.message_count
        test_log.info(f"Queue {rabbitmq_queue_name} have {message_count} new messages")

        if message_count > 0:
            message_found = await check_message_in_rabbitmq_queue(
                rabbitmq_queue_name,
                rabbitmq_channel,
                expected_message
            )

            if not message_found:
                pytest.fail("Message was not found on RabbitMQ")
        else:
            pytest.fail("No messages received on RabbitMQ")
    except Exception as e:
        pytest.fail(f"Failed to send message to RabbitMQ on_message callback: {e}")


@pytest.mark.asyncio
async def test_on_message_environment(
        gmqtt_client,
        use_ingestor_connected,
        get_rabbitmq_config,
        use_rabbitmq_client,
        test_log
):

    test_log.info("MQTT Ingestor started")
    ingestor = use_ingestor_connected

    _, rabbitmq_queue_name = get_rabbitmq_config
    rabbitmq_connection, rabbitmq_channel = use_rabbitmq_client

    topic = "scgdi/motor/environment"
    payload = {
        "timestamp": datetime.now().isoformat(),
        "temperature": 36.6,
        "humidity": 58.2,
        "caseTemperature": 60.4
    }

    expected_message = {
        "timestamp": datetime.now().isoformat(),
        "topic": topic,
        "data": payload
    }

    try:
        test_log.info("Publishing on MQTT environment motor information")
        gmqtt_client.publish(topic, json.dumps(payload))
        await asyncio.sleep(0.5)

        test_log.info("Checking if RabbitMQ receive new messages")
        queue = await rabbitmq_channel.declare_queue(rabbitmq_queue_name, passive=True)
        message_count = queue.declaration_result.message_count
        test_log.info(f"Queue {rabbitmq_queue_name} have {message_count} new messages")

        if message_count > 0:
            message_found = await check_message_in_rabbitmq_queue(
                rabbitmq_queue_name,
                rabbitmq_channel,
                expected_message
            )

            if not message_found:
                pytest.fail("Message was not found on RabbitMQ")
        else:
            pytest.fail("No messages received on RabbitMQ")
    except Exception as e:
        pytest.fail(f"Failed to send message to RabbitMQ on_message callback: {e}")


@pytest.mark.asyncio
async def test_on_message_vibration(
        gmqtt_client,
        use_ingestor_connected,
        get_rabbitmq_config,
        use_rabbitmq_client,
        test_log
):

    test_log.info("MQTT Ingestor started")
    ingestor = use_ingestor_connected

    _, rabbitmq_queue_name = get_rabbitmq_config
    rabbitmq_connection, rabbitmq_channel = use_rabbitmq_client

    topic = "scgdi/motor/vibration"
    payload = {
        "timestamp": datetime.now().isoformat(),
        "axial": 0.12,
        "radial": 0.15
    }

    expected_message = {
        "timestamp": datetime.now().isoformat(),
        "topic": topic,
        "data": payload
    }

    try:
        test_log.info("Publishing on MQTT vibration motor information")
        gmqtt_client.publish(topic, json.dumps(payload))
        await asyncio.sleep(0.5)

        test_log.info("Checking if RabbitMQ receive new messages")
        queue = await rabbitmq_channel.declare_queue(rabbitmq_queue_name, passive=True)
        message_count = queue.declaration_result.message_count
        test_log.info(f"Queue {rabbitmq_queue_name} have {message_count} new messages")

        if message_count > 0:
            message_found = await check_message_in_rabbitmq_queue(
                rabbitmq_queue_name,
                rabbitmq_channel,
                expected_message
            )

            if not message_found:
                pytest.fail("Message was not found on RabbitMQ")
        else:
            pytest.fail("No messages received on RabbitMQ")
    except Exception as e:
        pytest.fail(f"Failed to send message to RabbitMQ on_message callback: {e}")
