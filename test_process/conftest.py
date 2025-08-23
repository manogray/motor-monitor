import pytest
import sys
import gmqtt
import asyncio
import importlib.util

from asyncua import Client, ua
from pathlib import Path


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
def get_opcua_configs(settings):
    return settings["OPCUA_SERVER"], settings["OPCUA_NAMESPACE"]

