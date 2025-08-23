import pytest
import asyncio
import json

from asyncua import Client, ua
from datetime import datetime
from deepdiff import DeepDiff


@pytest.mark.asyncio
async def test_electrical_category(gmqtt_client, get_opcua_configs):
    opcua_server, opcua_namespace = get_opcua_configs

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

    gmqtt_client.publish(topic, json.dumps(payload))
    await asyncio.sleep(0.5)

    async with Client(opcua_server) as client:
        namespace_index = await client.get_namespace_index(opcua_namespace)
        
        voltage_a_node = client.get_node(ua.NodeId(5, namespace_index))
        voltage_b_node = client.get_node(ua.NodeId(6, namespace_index))
        voltage_c_node = client.get_node(ua.NodeId(7, namespace_index))
        current_a_node = client.get_node(ua.NodeId(8, namespace_index))
        current_b_node = client.get_node(ua.NodeId(9, namespace_index))
        current_c_node = client.get_node(ua.NodeId(10, namespace_index))
        power_active_node = client.get_node(ua.NodeId(11, namespace_index))
        power_reactive_node = client.get_node(ua.NodeId(12, namespace_index))
        power_apparent_node = client.get_node(ua.NodeId(13, namespace_index))
        energy_active_node = client.get_node(ua.NodeId(14, namespace_index))
        energy_reactive_node = client.get_node(ua.NodeId(15, namespace_index))
        energy_apparent_node = client.get_node(ua.NodeId(16, namespace_index))
        power_factor_node = client.get_node(ua.NodeId(17, namespace_index))
        frequency_node = client.get_node(ua.NodeId(18, namespace_index))

        data_from_opcua = {
            "voltage": {
                "a": await voltage_a_node.read_value(),
                "b": await voltage_b_node.read_value(),
                "c": await voltage_c_node.read_value()
            },
            "current": {
                "a": await current_a_node.read_value(),
                "b": await current_b_node.read_value(),
                "c": await current_c_node.read_value()
            },
            "power": {
                "active": await power_active_node.read_value(),
                "reactive": await power_reactive_node.read_value(),
                "apparent": await power_apparent_node.read_value()
            },
            "energy": {
                "active": await energy_active_node.read_value(),
                "reactive": await energy_reactive_node.read_value(),
                "apparent": await energy_apparent_node.read_value()
            },
            "powerFactor": await power_factor_node.read_value(),
            "frequency": await frequency_node.read_value()
        }

        payload.pop("timestamp")
        dict_difference = DeepDiff(payload, data_from_opcua)

        if dict_difference != {}:
            raise Exception(f"OPCUA Electrical nodes was not correctly updated: {dict_difference}")


@pytest.mark.asyncio
async def test_environment_category(gmqtt_client, get_opcua_configs):
    opcua_server, opcua_namespace = get_opcua_configs

    topic = "scgdi/motor/environment"
    payload = {
        "timestamp": datetime.now().isoformat(),
        "temperature": 34.5,
        "humidity": 55.2,
        "caseTemperature": 62.0
    }

    gmqtt_client.publish(topic, json.dumps(payload))
    await asyncio.sleep(0.5)

    async with Client(opcua_server) as client:
        namespace_index = await client.get_namespace_index(opcua_namespace)

        temperature_node = client.get_node(ua.NodeId(19, namespace_index))
        humidity_node = client.get_node(ua.NodeId(20, namespace_index))
        case_temperature_node = client.get_node(ua.NodeId(21, namespace_index))

        data_from_opcua = {
            "temperature": await temperature_node.read_value(),
            "humidity": await humidity_node.read_value(),
            "caseTemperature": await case_temperature_node.read_value()
        }

        payload.pop("timestamp")
        dict_difference = DeepDiff(payload, data_from_opcua)

        if dict_difference != {}:
            raise Exception(f"OPCUA Environment nodes was not correctly updated: {dict_difference}")


@pytest.mark.asyncio
async def test_vibration_category(gmqtt_client, get_opcua_configs):
    opcua_server, opcua_namespace = get_opcua_configs

    topic = "scgdi/motor/vibration"
    payload = {
        "timestamp": datetime.now().isoformat(),
        "axial": 0.12,
        "radial": 0.15
    }

    gmqtt_client.publish(topic, json.dumps(payload))
    await asyncio.sleep(0.5)

    async with Client(opcua_server) as client:
        namespace_index = await client.get_namespace_index(opcua_namespace)

        axial_node = client.get_node(ua.NodeId(22, namespace_index))
        radial_node = client.get_node(ua.NodeId(23, namespace_index))

        data_from_opcua = {
            "axial": await axial_node.read_value(),
            "radial": await radial_node.read_value()
        }

        payload.pop("timestamp")
        dict_difference = DeepDiff(payload, data_from_opcua)

        if dict_difference != {}:
            raise Exception(f"OPCUA Vibration nodes was not correctly updated: {dict_difference}")
