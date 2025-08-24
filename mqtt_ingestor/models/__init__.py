from pydantic import BaseModel
from typing import Dict


class ElectricalPayload(BaseModel):
    timestamp: str
    voltage: Dict[str, float]
    current: Dict[str, float]
    power: Dict[str, int]
    energy: Dict[str, int]
    powerFactor: float
    frequency: float


class EnvironmentPayload(BaseModel):
    timestamp: str
    temperature: float
    humidity: float
    caseTemperature: float


class VibrationPayload(BaseModel):
    timestamp: str
    axial: float
    radial: float
