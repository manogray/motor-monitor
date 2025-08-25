DB_PATH = "data/motor.db"

TORTOISE_ORM = {
    "connections": {
        "default": f"sqlite://{DB_PATH}"
    },
    "apps": {
        "models": {
            "models": ["models"],
            "default_connection": "default",
        }
    },
}
