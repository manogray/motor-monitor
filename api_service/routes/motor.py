from typing import List, Literal
from fastapi import APIRouter
from controllers.motor import get_variable_history, get_lastest_variable_changes, get_alarms_triggered
from schemas import LatestResponseSchema, AlarmsResponseSchema, VariableHistoryResponseSchema, VariableNameType

motor_router = APIRouter(prefix="/api/v1/motor", tags=["Equipments"])


@motor_router.get(
    path="/latest",
    response_model=LatestResponseSchema,
    summary="Get latest state of motor",
    description="Returns the latest values for all variables of motor"
)
async def get_latest() -> LatestResponseSchema:
    """
    Returns a list with electrical, environment and vibration variables with latest values
    """
    return await get_lastest_variable_changes()


@motor_router.get(
    path="/history/{variable}",
    response_model=List[VariableHistoryResponseSchema],
    summary="Get history of some given motor variable",
    description="List history of some specific motor variable by a period of time"
)
async def get_history(
        variable: VariableNameType,
        time_filter: Literal["24h", "3d", "7d"] = None
) -> List[VariableHistoryResponseSchema]:
    """
    Returns a list with some specific variable change by a period of time

    :param variable: Variable that will be retrieved the history
    :param time_filter: Time filter to slice history
    :return: List with variable history
    """
    return await get_variable_history(variable, time_filter)


@motor_router.get(
    path="/alarms",
    response_model=List[AlarmsResponseSchema],
    summary="Get a history of alarms triggered",
    description="List all alarms triggered"
)
async def get_alarms() -> List[AlarmsResponseSchema]:
    """
    Returns a list with all alarms triggered
    """
    return await get_alarms_triggered()
