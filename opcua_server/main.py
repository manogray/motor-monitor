import os
import asyncio
import json
import aio_pika

from log_recorder import logger
from asyncua import Server, ua
from database import SQLiteHistoryManager
from typing import Any


class OpcuaServer:
    """
    OPC UA server implementation for industrial motor monitoring
    """
    def __init__(self):
        self.config = dict(
            RABBITMQ_URL=os.getenv("RABBITMQ_URL"),
            EXCHANGE_NAME=os.getenv("EXCHANGE_NAME"),
            QUEUE_NAME=os.getenv("QUEUE_NAME"),
            URI=os.getenv("NAMESPACE"),
            ENDPOINT="opc.tcp://0.0.0.0:4840/motor50cv/"
        )
        self.rabbitmq_connection = None
        self.rabbitmq_connected = False
        self.nodeset = dict()
        self.alarm_conditions = dict()
        self.server = Server()
        self.index = None
        self.history_manager = SQLiteHistoryManager()

    async def setup_history(self):
        """
        Configure history to store variable changes

        :return: None
        """
        logger.info("Setting history")

        historical_nodes = [
            self.nodeset["motor50cv"]["electrical"]['voltage_a'],
            self.nodeset["motor50cv"]["electrical"]['voltage_b'],
            self.nodeset["motor50cv"]["electrical"]['voltage_c'],
            self.nodeset["motor50cv"]["electrical"]['current_a'],
            self.nodeset["motor50cv"]["electrical"]['current_b'],
            self.nodeset["motor50cv"]["electrical"]['current_c'],
            self.nodeset["motor50cv"]["environment"]["temperature"],
            self.nodeset["motor50cv"]["environment"]["humidity"],
            self.nodeset["motor50cv"]["environment"]['case_temperature'],
            self.nodeset["motor50cv"]["vibration"]["axial"],
            self.nodeset["motor50cv"]["vibration"]["radial"]
        ]

        await self.server.historize_node_data_change(historical_nodes)

    async def setup_alarms(self):
        """
        Configure alarms to be triggered

        :return: None
        """
        await self.create_alarm_condition(
            "OvervoltageAlarm",
            self.nodeset["motor50cv"]["electrical"]["obj"],
            self.nodeset["motor50cv"]["electrical"]["voltage_a"],
            lambda value: value > 242,
            "Overvoltage detected",
            700
        )

        await self.create_alarm_condition(
            "UndervoltageAlarm",
            self.nodeset["motor50cv"]["electrical"]["obj"],
            self.nodeset["motor50cv"]["electrical"]["voltage_a"],
            lambda value: value < 198,
            "Undervoltage detected",
            700
        )

        await self.create_alarm_condition(
            "OvercurrentAlarm",
            self.nodeset["motor50cv"]["electrical"]["obj"],
            self.nodeset["motor50cv"]["electrical"]["current_a"],
            lambda value: value > 11.55,
            "Overcurrent detected",
            700
        )

        await self.create_alarm_condition(
            "CaseTemperatureAlarm",
            self.nodeset["motor50cv"]["environment"]["obj"],
            self.nodeset["motor50cv"]["environment"]["case_temperature"],
            lambda value: value > 60,
            "Case temperature critical",
            900
        )

        logger.info(f"Alarms created: {self.alarm_conditions}")

    async def create_alarm_condition(self, name, object_node, source_node, condition, message, severity):
        """
        Create an alarm condition

        :param name:
        :param object_node:
        :param source_node:
        :param condition:
        :param message:
        :param severity:
        :return: None
        """
        logger.info(f"Creating {name} alarm")

        event_custom = await self.server.create_custom_event_type(self.index, name)
        event_custom_generator = await self.server.get_event_generator(event_custom, object_node)

        self.alarm_conditions[name] = {
            "condition": condition,
            "message": message,
            "severity": severity,
            "object": object_node,
            "source": source_node,
            "event_gen": event_custom_generator
        }

    async def check_alarms(self):
        """
        Checks alarm conditions periodically

        :return: None
        """
        while True:
            logger.info("Checking alarms")
            for name, alarm_info in self.alarm_conditions.items():
                logger.info(f"Check {name} alarm")
                current_value = await alarm_info['source'].get_value()
                if alarm_info['condition'](current_value):
                    await self.trigger_alarm(name, alarm_info, current_value)

            await asyncio.sleep(5)

    async def trigger_alarm(self, name, alarm_info, current_value):
        """
        Trigger an alarm

        :param name:
        :param alarm_info:
        :param current_value:
        :return: None
        """
        event_gen = alarm_info.get("event_gen")
        event_gen.event.Severity = alarm_info["severity"]
        event_gen.event.Message = ua.LocalizedText(f"{alarm_info['message']}. Current value: {current_value}")
        event_gen.event.SourceNode = alarm_info["source"].nodeid
        await event_gen.trigger()

        self.history_manager.store_event_alarm(
            name,
            alarm_info["severity"],
            f"{alarm_info['message']}. Current value: {current_value}",
            str(alarm_info["source"].nodeid)
        )

        logger.warning(f"Alarm triggered: {name} - {alarm_info['message']}")

    async def connect_rabbitmq(self, attempts: int = 15) -> bool:
        """
        Establish connection to RabbitMQ with retry logic

        :param attempts: Number of attempts to try to connect to RabbitMQ
        :return: True if connection was successfully established
        """
        for attempt in range(attempts):
            try:
                self.rabbitmq_connection = await aio_pika.connect_robust(self.config["RABBITMQ_URL"])
                self.rabbitmq_connected = True

                return self.rabbitmq_connected
            except Exception as e:
                logger.warning(f"Trying to connect to RabbitMQ - attempt [{attempt+1}/{attempts}]")
                logger.warning(f"Failed: {e}")
                await asyncio.sleep(5)

        logger.error("Not possible to connect to RabbitMQ")
        return self.rabbitmq_connected

    async def consume_rabbitmq(self) -> None:
        """
        Continuous consumer for RabbitMQ messages containing sensor data

        :return: None
        """
        await self.connect_rabbitmq()

        if not self.rabbitmq_connected or self.rabbitmq_connection is None:
            logger.error("RabbitMQ not connected. Not possible to consume messages")
            return

        async with self.rabbitmq_connection:
            channel = await self.rabbitmq_connection.channel()
            exchange = await channel.declare_exchange(
                self.config["EXCHANGE_NAME"],
                aio_pika.ExchangeType.TOPIC,
                durable=True
            )
            queue = await channel.declare_queue(self.config["QUEUE_NAME"], durable=True)
            await queue.bind(exchange, routing_key="motor.*")
            logger.info("RabbitMQ consumer started. Waiting for new messages...")

            async with queue.iterator() as queue_iter:
                async for msg in queue_iter:
                    async with msg.process():
                        try:
                            data = json.loads(msg.body.decode())
                            routing_key = msg.routing_key
                            logger.info(f"Received message ({routing_key}): {data}")

                            success = await self.update_nodes(routing_key, data)

                            if success:
                                logger.info("Nodes updated")
                            else:
                                logger.warning(f"Not possible to update nodes to {routing_key}")

                        except json.JSONDecodeError as e:
                            logger.error(f"Not possible to decode JSON: {e}")
                        except Exception as e:
                            logger.error(f"Not possible to process received message: {e}")

    @staticmethod
    async def set_node_writable(node_list: dict) -> None:
        """
        Set OPCUA node variables as writables

        :param node_list: Dictionary with OPCUA node variables
        :return: None
        """
        for key, value in node_list.items():
            if key != "obj":
                await value.set_writable()

    async def setup_nodeset(self) -> None:
        """
        Configure the nodeset tree of OPCUA server to monitor industrial motor

        :return: None
        """
        self.nodeset.update({"motor50cv": {
            "obj": await self.server.nodes.objects.add_object(ua.NodeId(1, self.index), bname="Motor50CV")
        }})
        self.nodeset["motor50cv"].update({"electrical": {
            "obj": await self.nodeset["motor50cv"]["obj"].add_object(ua.NodeId(2, self.index), bname="Electrical")
        }})
        self.nodeset["motor50cv"].update({"environment": {
            "obj": await self.nodeset["motor50cv"]["obj"].add_object(ua.NodeId(3, self.index), bname="Environment")
        }})
        self.nodeset["motor50cv"].update({"vibration": {
            "obj": await self.nodeset["motor50cv"]["obj"].add_object(ua.NodeId(4, self.index), bname="Vibration")
        }})

        self.nodeset["motor50cv"]["electrical"].update({
            "voltage_a": await self.nodeset["motor50cv"]["electrical"]["obj"].add_variable(
                ua.NodeId(5, self.index),
                bname="VoltageA",
                val=0.0
            )
        })
        self.nodeset["motor50cv"]["electrical"].update({
            "voltage_b": await self.nodeset["motor50cv"]["electrical"]["obj"].add_variable(
                ua.NodeId(6, self.index),
                bname="VoltageB",
                val=0.0
            )})
        self.nodeset["motor50cv"]["electrical"].update({
            "voltage_c": await self.nodeset["motor50cv"]["electrical"]["obj"].add_variable(
                ua.NodeId(7, self.index),
                bname="VoltageC",
                val=0.0
            )})
        self.nodeset["motor50cv"]["electrical"].update({
            "current_a": await self.nodeset["motor50cv"]["electrical"]["obj"].add_variable(
                ua.NodeId(8, self.index),
                bname="CurrentA",
                val=0.0
            )})
        self.nodeset["motor50cv"]["electrical"].update({
            "current_b": await self.nodeset["motor50cv"]["electrical"]["obj"].add_variable(
                ua.NodeId(9, self.index),
                bname="CurrentB",
                val=0.0
            )})
        self.nodeset["motor50cv"]["electrical"].update({
            "current_c": await self.nodeset["motor50cv"]["electrical"]["obj"].add_variable(
                ua.NodeId(10, self.index),
                bname="CurrentC",
                val=0.0
            )})
        self.nodeset["motor50cv"]["electrical"].update({
            "power_active": await self.nodeset["motor50cv"]["electrical"]["obj"].add_variable(
                ua.NodeId(11, self.index),
                bname="PowerActive",
                val=0
            )})
        self.nodeset["motor50cv"]["electrical"].update({
            "power_reactive": await self.nodeset["motor50cv"]["electrical"]["obj"].add_variable(
                ua.NodeId(12, self.index),
                bname="PowerReactive",
                val=0
            )})
        self.nodeset["motor50cv"]["electrical"].update({
            "power_apparent": await self.nodeset["motor50cv"]["electrical"]["obj"].add_variable(
                ua.NodeId(13, self.index),
                bname="PowerApparent",
                val=0
            )})
        self.nodeset["motor50cv"]["electrical"].update({
            "energy_active": await self.nodeset["motor50cv"]["electrical"]["obj"].add_variable(
                ua.NodeId(14, self.index),
                bname="EnergyActive",
                val=0
            )})
        self.nodeset["motor50cv"]["electrical"].update({
            "energy_reactive": await self.nodeset["motor50cv"]["electrical"]["obj"].add_variable(
                ua.NodeId(15, self.index),
                bname="EnergyReactive",
                val=0
            )})
        self.nodeset["motor50cv"]["electrical"].update({
            "energy_apparent": await self.nodeset["motor50cv"]["electrical"]["obj"].add_variable(
                ua.NodeId(16, self.index),
                bname="EnergyApparent",
                val=0
            )})
        self.nodeset["motor50cv"]["electrical"].update({
            "power_factor": await self.nodeset["motor50cv"]["electrical"]["obj"].add_variable(
                ua.NodeId(17, self.index),
                bname="PowerFactor",
                val=0.0
            )})
        self.nodeset["motor50cv"]["electrical"].update({
            "frequency": await self.nodeset["motor50cv"]["electrical"]["obj"].add_variable(
                ua.NodeId(18, self.index),
                bname="Frequency",
                val=0.0
            )})
        
        self.nodeset["motor50cv"]["environment"].update({
            "temperature": await self.nodeset["motor50cv"]["environment"]["obj"].add_variable(
                ua.NodeId(19, self.index),
                bname="Temperature",
                val=0.0
            )})
        self.nodeset["motor50cv"]["environment"].update({
            "humidity": await self.nodeset["motor50cv"]["environment"]["obj"].add_variable(
                ua.NodeId(20, self.index),
                bname="Humidity",
                val=0.0
            )})
        self.nodeset["motor50cv"]["environment"].update({
            "case_temperature": await self.nodeset["motor50cv"]["environment"]["obj"].add_variable(
                ua.NodeId(21, self.index),
                bname="CaseTemperature",
                val=0.0
            )})
        
        self.nodeset["motor50cv"]["vibration"].update({
            "axial": await self.nodeset["motor50cv"]["vibration"]["obj"].add_variable(
                ua.NodeId(22, self.index),
                bname="Axial",
                val=0.0
            )})
        self.nodeset["motor50cv"]["vibration"].update({
            "radial": await self.nodeset["motor50cv"]["vibration"]["obj"].add_variable(
                ua.NodeId(23, self.index),
                bname="Radial",
                val=0.0
            )})

        await self.set_node_writable(self.nodeset["motor50cv"]["electrical"])
        await self.set_node_writable(self.nodeset["motor50cv"]["environment"])
        await self.set_node_writable(self.nodeset["motor50cv"]["vibration"])

    async def update_nodes(self, routing_key: str, data: dict) -> bool:
        """
        Update OPCUA nodes with given data

        :param routing_key: RabbitMQ routing key indicating data source
        :param data: Data to be updated on OPCUA nodes
        :return: True if nodes was successfully updated
        """
        updated = False
        update_data = data.get("data", {})

        try:
            electrical_node = self.nodeset["motor50cv"]["electrical"]
            environment_node = self.nodeset["motor50cv"]["environment"]
            vibration_node = self.nodeset["motor50cv"]["vibration"]

            if "electrical" in routing_key:
                voltage = update_data.get("voltage", {})
                current = update_data.get("current", {})
                power = update_data.get("power", {})
                energy = update_data.get("energy", {})

                if voltage:
                    if "a" in current:
                        if await electrical_node["voltage_a"].read_value() != voltage["a"]:
                            name_class = await electrical_node["voltage_a"].read_browse_name()
                            self.history_manager.store_variable_change(
                                electrical_node["voltage_a"].nodeid,
                                name_class.Name,
                                voltage["a"]
                            )

                        await electrical_node["voltage_a"].write_value(voltage["a"])
                        updated = True
                    if "b" in voltage:
                        if await electrical_node["voltage_b"].read_value() != voltage["b"]:
                            name_class = await electrical_node["voltage_b"].read_browse_name()
                            self.history_manager.store_variable_change(
                                electrical_node["voltage_b"].nodeid,
                                name_class.Name,
                                voltage["b"]
                            )

                        await electrical_node["voltage_b"].write_value(voltage["b"])
                        updated = True
                    if "c" in voltage:
                        if await electrical_node["voltage_c"].read_value() != voltage["c"]:
                            name_class = await electrical_node["voltage_c"].read_browse_name()
                            self.history_manager.store_variable_change(
                                electrical_node["voltage_c"].nodeid,
                                name_class.Name,
                                voltage["c"]
                            )

                        await electrical_node["voltage_c"].write_value(voltage["c"])
                        updated = True
                if current:
                    if "a" in current:
                        if await electrical_node["current_a"].read_value() != current["a"]:
                            name_class = await electrical_node["current_a"].read_browse_name()
                            self.history_manager.store_variable_change(
                                electrical_node["current_a"].nodeid,
                                name_class.Name,
                                current["a"]
                            )

                        await electrical_node["current_a"].write_value(current["a"])
                        updated = True
                    if "b" in current:
                        if await electrical_node["current_b"].read_value() != current["b"]:
                            name_class = await electrical_node["current_b"].read_browse_name()
                            self.history_manager.store_variable_change(
                                electrical_node["current_b"].nodeid,
                                name_class.Name,
                                current["b"]
                            )

                        await electrical_node["current_b"].write_value(current["b"])
                        updated = True
                    if "c" in current:
                        if await electrical_node["current_c"].read_value() != current["c"]:
                            name_class = await electrical_node["current_c"].read_browse_name()
                            self.history_manager.store_variable_change(
                                electrical_node["current_c"].nodeid,
                                name_class.Name,
                                current["c"]
                            )

                        await electrical_node["current_c"].write_value(current["c"])
                        updated = True
                if power:
                    if "active" in power:
                        if await electrical_node["power_active"].read_value() != power["active"]:
                            name_class = await electrical_node["power_active"].read_browse_name()
                            self.history_manager.store_variable_change(
                                electrical_node["power_active"].nodeid,
                                name_class.Name,
                                power["active"]
                            )

                        await electrical_node["power_active"].write_value(power["active"])
                        updated = True
                    if "reactive" in power:
                        if await electrical_node["power_reactive"].read_value() != power["reactive"]:
                            name_class = await electrical_node["power_reactive"].read_browse_name()
                            self.history_manager.store_variable_change(
                                electrical_node["power_reactive"].nodeid,
                                name_class.Name,
                                power["reactive"]
                            )

                        await electrical_node["power_reactive"].write_value(power["reactive"])
                        updated = True
                    if "apparent" in power:
                        if await electrical_node["power_apparent"].read_value() != power["apparent"]:
                            name_class = await electrical_node["power_apparent"].read_browse_name()
                            self.history_manager.store_variable_change(
                                electrical_node["power_apparent"].nodeid,
                                name_class.Name,
                                power["apparent"]
                            )

                        await electrical_node["power_apparent"].write_value(power["apparent"])
                        updated = True
                if energy:
                    if "active" in energy:
                        if await electrical_node["energy_active"].read_value() != energy["active"]:
                            name_class = await electrical_node["energy_active"].read_browse_name()
                            self.history_manager.store_variable_change(
                                electrical_node["energy_active"].nodeid,
                                name_class.Name,
                                energy["active"]
                            )

                        await electrical_node["energy_active"].write_value(energy["active"])
                        updated = True
                    if "reactive" in energy:
                        if await electrical_node["energy_reactive"].read_value() != energy["reactive"]:
                            name_class = await electrical_node["energy_reactive"].read_browse_name()
                            self.history_manager.store_variable_change(
                                electrical_node["energy_reactive"].nodeid,
                                name_class.Name,
                                energy["reactive"]
                            )

                        await electrical_node["energy_reactive"].write_value(energy["reactive"])
                        updated = True
                    if "apparent" in energy:
                        if await electrical_node["energy_apparent"].read_value() != energy["apparent"]:
                            name_class = await electrical_node["energy_apparent"].read_browse_name()
                            self.history_manager.store_variable_change(
                                electrical_node["energy_apparent"].nodeid,
                                name_class.Name,
                                energy["apparent"]
                            )

                        await electrical_node["energy_apparent"].write_value(energy["apparent"])
                        updated = True

                if "powerFactor" in update_data:
                    if await electrical_node["power_factor"].read_value() != update_data["powerFactor"]:
                        name_class = await electrical_node["power_factor"].read_browse_name()
                        self.history_manager.store_variable_change(
                            electrical_node["power_factor"].nodeid,
                            name_class.Name,
                            update_data["powerFactor"]
                        )

                    await electrical_node["power_factor"].write_value(update_data["powerFactor"])
                    updated = True

                if "frequency" in update_data:
                    if await electrical_node["frequency"].read_value() != update_data["frequency"]:
                        name_class = await electrical_node["frequency"].read_browse_name()
                        self.history_manager.store_variable_change(
                            electrical_node["frequency"].nodeid,
                            name_class.Name,
                            update_data["frequency"]
                        )

                    await electrical_node["frequency"].write_value(update_data["frequency"])
                    updated = True
            elif "environment" in routing_key:
                if "temperature" in update_data:
                    if await environment_node["temperature"].read_value() != update_data["temperature"]:
                        name_class = await environment_node["temperature"].read_browse_name()
                        self.history_manager.store_variable_change(
                            environment_node["temperature"].nodeid,
                            name_class.Name,
                            update_data["temperature"]
                        )

                    await environment_node["temperature"].write_value(update_data["temperature"])
                    updated = True

                if "humidity" in update_data:
                    if await environment_node["humidity"].read_value() != update_data["humidity"]:
                        name_class = await environment_node["humidity"].read_browse_name()
                        self.history_manager.store_variable_change(
                            environment_node["humidity"].nodeid,
                            name_class.Name,
                            update_data["humidity"]
                        )

                    await environment_node["humidity"].write_value(update_data["humidity"])
                    updated = True

                if "caseTemperature" in update_data:
                    if await environment_node["case_temperature"].read_value() != update_data["caseTemperature"]:
                        name_class = await environment_node["case_temperature"].read_browse_name()
                        self.history_manager.store_variable_change(
                            environment_node["case_temperature"].nodeid,
                            name_class.Name,
                            update_data["caseTemperature"]
                        )

                    await environment_node["case_temperature"].write_value(update_data["caseTemperature"])
                    updated = True
            elif "vibration" in routing_key:
                if "axial" in update_data:
                    if await vibration_node["axial"].read_value() != update_data["axial"]:
                        name_class = await vibration_node["axial"].read_browse_name()
                        self.history_manager.store_variable_change(
                            vibration_node["axial"].nodeid,
                            name_class.Name,
                            update_data["axial"]
                        )

                    await vibration_node["axial"].write_value(update_data["axial"])
                    updated = True
                
                if "radial" in update_data:
                    if await vibration_node["radial"].read_value() != update_data["radial"]:
                        name_class = await vibration_node["radial"].read_browse_name()
                        self.history_manager.store_variable_change(
                            vibration_node["radial"].nodeid,
                            name_class.Name,
                            update_data["radial"]
                        )

                    await vibration_node["radial"].write_value(update_data["radial"])
                    updated = True

            return updated

        except Exception as e:
            logger.error(f"Not possible to update OPCUA nodes: {e}")
            return False


async def main():
    """
    Main function to be executed on this microservice

    :return:
    """
    opcua = OpcuaServer()
    logger.info(f"Configs used: {opcua.config}")

    await opcua.server.init()
    opcua.server.set_endpoint(opcua.config["ENDPOINT"])
    opcua.index = await opcua.server.register_namespace(opcua.config["URI"])

    logger.info("Creating nodeset")
    await opcua.setup_nodeset()

    await opcua.setup_history()

    await opcua.setup_alarms()

    asyncio.create_task(opcua.consume_rabbitmq())

    asyncio.create_task(opcua.check_alarms())

    logger.info("Starting server...")
    async with opcua.server:
        while True:
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
