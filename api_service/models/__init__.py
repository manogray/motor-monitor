from pydantic import BaseModel
from datetime import datetime


class ElectricalData(BaseModel):
    timestamp: datetime
    voltage_a: float
    voltage_b: float
    voltage_c: float
    current_a: float
    current_b: float
    current_c: float
    power_active: float
    power_reactive: float
    power_apparent: float
    energy_active: float
    energy_reactive: float
    energy_apparent: float
    power_factor: float
    frequency: float


class EnvironmentData(BaseModel):
    timestamp: datetime
    temperature: float
    humidity: float
    case_temperature: float


class VibrationData(BaseModel):
    timestamp: datetime
    axial: float
    radial: float


class AlarmEvent(BaseModel):
    id: int
    timestamp: datetime
    severity: int
    message: str
    variable: str
    value: float
