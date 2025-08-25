from models import VariableChanges, Events
from datetime import datetime, timedelta
from typing import List
from schemas import (VariableHistoryResponseSchema, LatestResponseSchema, ElectricalSchema, EnvironmentSchema,
                     VibrationSchema, VoltageSchema, CurrentSchema, PowerSchema, EnergySchema, AlarmsResponseSchema)


async def get_lastest_variable_changes() -> LatestResponseSchema:
    """
    Returns a list with latest motor variable values
    """
    history = await VariableChanges.raw('''
    SELECT vc.*
    FROM variable_changes vc
    JOIN (
        SELECT variable_name, MAX(timestamp) AS max_ts
        FROM variable_changes
        GROUP BY variable_name
    ) latest
    ON vc.variable_name = latest.variable_name
    AND vc.timestamp = latest.max_ts LIMIT 100
    ''')

    voltage_a = next((row for row in history if row.variable_name == "VoltageA"), None)
    voltage_b = next((row for row in history if row.variable_name == "VoltageB"), None)
    voltage_c = next((row for row in history if row.variable_name == "VoltageC"), None)
    current_a = next((row for row in history if row.variable_name == "CurrentA"), None)
    current_b = next((row for row in history if row.variable_name == "CurrentB"), None)
    current_c = next((row for row in history if row.variable_name == "CurrentC"), None)
    power_active = next((row for row in history if row.variable_name == "PowerActive"), None)
    power_reactive = next((row for row in history if row.variable_name == "PowerReactive"), None)
    power_apparent = next((row for row in history if row.variable_name == "PowerApparent"), None)
    energy_active = next((row for row in history if row.variable_name == "EnergyActive"), None)
    energy_reactive = next((row for row in history if row.variable_name == "EnergyReactive"), None)
    energy_apparent = next((row for row in history if row.variable_name == "EnergyApparent"), None)
    power_factor = next((row for row in history if row.variable_name == "PowerFactor"), None)
    frequency = next((row for row in history if row.variable_name == "Frequency"), None)

    temperature = next((row for row in history if row.variable_name == "Temperature"), None)
    humidity = next((row for row in history if row.variable_name == "Humidity"), None)
    case_temperature = next((row for row in history if row.variable_name == "CaseTemperature"), None)

    axial = next((row for row in history if row.variable_name == "Axial"), None)
    radial = next((row for row in history if row.variable_name == "Radial"), None)

    voltage_response = VoltageSchema(
        a=voltage_a.value,
        b=voltage_b.value,
        c=voltage_c.value
    )

    current_response = CurrentSchema(
        a=current_a.value,
        b=current_b.value,
        c=current_c.value
    )

    power_response = PowerSchema(
        active=power_active.value,
        reactive=power_reactive.value,
        apparent=power_apparent.value
    )

    energy_response = EnergySchema(
        active=energy_active.value,
        reactive=energy_reactive.value,
        apparent=energy_apparent.value
    )

    electrical_response = ElectricalSchema(
        voltage=voltage_response,
        current=current_response,
        power=power_response,
        energy=energy_response,
        power_factor=power_factor.value,
        frequency=frequency.value
    )

    environment_response = EnvironmentSchema(
        temperature=temperature.value,
        humidity=humidity.value,
        case_temperature=case_temperature.value
    )

    vibration_response = VibrationSchema(
        axial=axial.value,
        radial=radial.value
    )

    response = LatestResponseSchema(
        electrical=electrical_response,
        environment=environment_response,
        vibration=vibration_response
    )

    return response


async def get_variable_history(variable: str, time_filter: str) -> List[VariableHistoryResponseSchema]:
    """
    Returns a list with a motor variable history

    :param variable: Variable that will be retrieved the history
    :param time_filter: Time filter to slice history
    :return: List with variable history
    """
    response = []

    if time_filter == "24h":
        changes = await VariableChanges.filter(
            variable_name=variable,
            timestamp__gte=datetime.now() - timedelta(days=7)
        )
    elif time_filter == "3d":
        changes = await VariableChanges.filter(
            variable_name=variable,
            timestamp__gte=datetime.now() - timedelta(days=7)
        )
    elif time_filter == "7d":
        changes = await VariableChanges.filter(
            variable_name=variable,
            timestamp__gte=datetime.now() - timedelta(days=7)
        )
    else:
        changes = await VariableChanges.filter(
            variable_name=variable
        ).all()

    for change in changes:
        response.append(VariableHistoryResponseSchema(
            variable_name=change.variable_name,
            value=change.value,
            timestamp=change.timestamp
        ))

    return response


async def get_alarms_triggered() -> List[AlarmsResponseSchema]:
    """
    Returns all alarms triggered
    """
    alarms = await Events.all().order_by("-timestamp")

    response_alarms = []

    for alarm in alarms:
        response_alarms.append(AlarmsResponseSchema(
            name=alarm.name,
            severity=alarm.severity,
            message=alarm.message,
            timestamp=alarm.timestamp
        ))

    return response_alarms
