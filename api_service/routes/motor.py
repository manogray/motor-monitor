from fastapi import APIRouter

motor_router = APIRouter(prefix="/api/v1/motor", tags=["Equipments"])

@motor_router.get("/latest")
async def get_latest():
    return {
        "status": "ok",
        "data": "latest"
    }

@motor_router.get("/history/{variable}")
async def get_history(variable: str):
    return {
        "variable": variable,
        "history": []
    }

@motor_router.get("/alarms")
async def get_alarms():
    return {
        "alarms": []
    }