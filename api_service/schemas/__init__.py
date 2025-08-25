from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum


class VoltageSchema(BaseModel):
    a: float = Field(..., description="Voltage A")
    b: float = Field(..., description="Voltage B")
    c: float = Field(..., description="Voltage C")


class CurrentSchema(BaseModel):
    a: float = Field(..., description="Current A")
    b: float = Field(..., description="Current B")
    c: float = Field(..., description="Current C")


class PowerSchema(BaseModel):
    active: int = Field(..., description="Power active")
    reactive: int = Field(..., description="Power reactive")
    apparent: int = Field(..., description="Power apparent")


class EnergySchema(BaseModel):
    active: int = Field(..., description="Energy active")
    reactive: int = Field(..., description="Energy reactive")
    apparent: int = Field(..., description="Energy apparent")


class ElectricalSchema(BaseModel):
    voltage: VoltageSchema = Field(..., description="3 phases of voltage motor")
    current: CurrentSchema = Field(..., description="3 phases of current motor")
    power: PowerSchema = Field(..., description="Active, Reactive and Apparent Power of motor")
    energy: EnergySchema = Field(..., description="Active, Reactive and Apparent Energy of motor")
    power_factor: float = Field(..., description="Power factor of motor")
    frequency: float = Field(..., description="Frequency of motor power supply")


class EnvironmentSchema(BaseModel):
    temperature: float = Field(..., description="Temperature of motor environment")
    humidity: float = Field(..., description="Humidity of motor environment")
    case_temperature: float = Field(..., description="Temperature of motor case")


class VibrationSchema(BaseModel):
    axial: float = Field(..., description="Axial vibration of motor")
    radial: float = Field(..., description="Radial vibration of motor")


class LatestResponseSchema(BaseModel):
    electrical: ElectricalSchema = Field(..., description="Electrical variables of motor")
    environment: EnvironmentSchema = Field(..., description="Environment variables of motor")
    vibration: VibrationSchema = Field(..., description="Vibration variables of motor")


class AlarmsResponseSchema(BaseModel):
    name: str = Field(..., description="Name of alarm")
    severity: int = Field(..., description="Severity of alarm")
    message: str = Field(..., description="Message of alarm")
    timestamp: datetime = Field(..., description="Datetime where alarm was triggered")


class VariableHistoryResponseSchema(BaseModel):
    variable_name: str = Field(..., description="Name of motor variable")
    value: int | float = Field(..., description="Value of motor variable")
    timestamp: datetime = Field(..., description="Datetime when variable value was registered")


class VariableNameType(str, Enum):
    voltage_a = "VoltageA"
    voltage_b = "VoltageB"
    voltage_c = "VoltageC"
    current_a = "CurrentB"
    current_b = "CurrentB"
    current_c = "CurrentB"
    power_active = "PowerActive"
    power_reactive = "PowerReactive"
    power_apparent = "PowerApparent"
    energy_active = "EnergyActive"
    energy_reactive = "EnergyReactive"
    energy_apparent = "EnergyApparent"
    power_factor = "PowerFactor"
    frequency = "Frequency"
    temperature = "Temperature"
    humidity = "Humidity"
    case_temperature = "CaseTemperature"
    axial = "Axial"
    radial = "Radial"
