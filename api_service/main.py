from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routes.motor import motor_router

allowed_cross_origins = ["*"]

app = FastAPI(
    title="Motor 50cv Monitor API",
    version="1.0.0",
    description="API that provides data about Motor 50cv monitoring",
    terms_of_service="/terms"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_cross_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

app.include_router(motor_router)
