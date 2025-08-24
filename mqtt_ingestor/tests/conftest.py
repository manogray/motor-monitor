import pytest
import sys
import gmqtt
import asyncio
import importlib.util
import aio_pika
import logging

from pathlib import Path
from mqtt_ingestor.main import MqttIngestor


def pytest_addoption(parser):
    parser.addoption(
        "--settings",
        action="store",
        default=None,
        help="Path to test configurations file"
    )


def load_settings_from_file(file_path):
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"settings file not found: {file_path}")

    spec = importlib.util.spec_from_file_location("custom_settings", file_path)
    if spec is None:
        raise ImportError(f"not possible to load spec file: {file_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules["custom_settings"] = module

    try:
        spec.loader.exec_module(module)
    except Exception as e:
        raise ImportError(f"not possible to execute settings module: {e}")

    settings = {}
    for name in dir(module):
        if not name.startswith('_'):
            value = getattr(module, name)
            if not (callable(value) or isinstance(value, type) or hasattr(value, '__module__')):
                settings[name] = value

    return settings


@pytest.fixture(scope="session")
def settings(pytestconfig):
    settings_file = pytestconfig.getoption("--settings")

    try:
        return load_settings_from_file(settings_file)
    except Exception as e:
        pytest.fail(f"Error to load settings: {e}")


@pytest.fixture(scope="session")
def test_log():
    """
    Configure a log to be used on tests
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    return logger


@pytest.fixture(scope="session")
async def gmqtt_client(settings):
    """
    Retrieves MQTT client to perform tests

    :param settings: reference to config variables of test_setup.py
    :return: MQTT client instance
    """
    client = gmqtt.Client("test-publisher")
    connected = asyncio.Event()

    def on_connect(client, flags, rc, properties):
        connected.set()

    client.on_connect = on_connect

    try:
        await client.connect(settings["MQTT_BROKER"], settings["MQTT_PORT"])
        await asyncio.wait_for(connected.wait(), timeout=10)

        yield client
    except asyncio.TimeoutError:
        pytest.fail("Timeout to connect to MQTT broker")
    except Exception as e:
        pytest.fail(f"Not possible to connect MQTT broker: {e}")
    finally:
        try:
            await client.disconnect()
        except:
            pass


@pytest.fixture(scope="session")
def get_rabbitmq_config(settings):
    """
    Retrieve host URL and queue from RabbitMQ

    :return: A tuple with RabbitMQ URL and RabbitMQ Queue name
    """
    return settings["RABBITMQ_URL"], settings["RABBITMQ_QUEUE_NAME"]


@pytest.fixture
def get_ingestor_instance(settings):
    """
    Create an MQTT Ingestor instance

    :return: MQTT Ingestor instance
    """
    ingestor = MqttIngestor({
        "broker": settings["MQTT_BROKER"],
        "port": settings["MQTT_PORT"],
        "rabbitmq_url": settings["RABBITMQ_URL"],
        "rabbitmq_exchange": settings["RABBITMQ_EXCHANGE_NAME"],
        "rabbitmq_queue_name": settings["RABBITMQ_QUEUE_NAME"]
    })

    return ingestor


@pytest.fixture
async def use_ingestor_connected(get_ingestor_instance):
    """
    Create an MQTT Ingestor instance and connect it to broker

    :return: MQTT Ingestor instance
    """
    ingestor = get_ingestor_instance

    await ingestor.connect_to_broker()

    yield ingestor

    await ingestor.mqtt_client.disconnect()


@pytest.fixture
async def use_rabbitmq_client(get_rabbitmq_config):
    """
    Create a RabbitMQ client and connect it to host

    :return: A Tuple with RabbitMQ client instance and channel
    """
    rabbit_url, rabbit_queue = get_rabbitmq_config
    connection = None
    try:
        connection = await aio_pika.connect_robust(rabbit_url)
        channel = await connection.channel()

        yield connection, channel

    except Exception as e:
        pytest.fail(f"Not possible to connect to RabbitMQ host: {e}")
    finally:
        await connection.close()
