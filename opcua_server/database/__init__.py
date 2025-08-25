import logging
import sqlite3

from datetime import datetime
from asyncua import ua

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("opcua-server")


class SQLiteHistoryManager:
    """
        Implements SQLite history based to be used on OPCUA server
    """
    def __init__(self, db_path="data/motor.db"):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """
        Initialize SQLite database
        """
        with sqlite3.connect(self.db_path) as connection:
            cursor = connection.cursor()

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS variable_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id TEXT NOT NULL,
                variable_name TEXT NOT NULL,
                timestamp DATETIME NOT NULL,
                value REAL NOT NULL
            )
            ''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                severity INTEGER NOT NULL,
                message TEXT NOT NULL,
                timestamp DATETIME NOT NULL,
                source_node TEXT NOT NULL
            )
            ''')

            cursor.execute("CREATE INDEX IF NOT EXISTS idx_node_id ON variable_changes(node_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON variable_changes(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_event_timestamp ON events(timestamp)")

            connection.commit()

    def store_variable_change(self, node_id: ua.NodeId, variable_name: str, value: float):
        """
        Store a variable change on database

        :param node_id: Node id reference of variable
        :param variable_name: BrowseName of variable
        :param value: value changed
        :return: None
        """
        try:
            with sqlite3.connect(self.db_path) as connection:
                cursor = connection.cursor()
                cursor.execute('''
                INSERT INTO variable_changes
                (node_id, variable_name, timestamp, value)
                VALUES (?, ?, ?, ?)
                ''', (str(node_id), variable_name, datetime.now().isoformat(), value))

                connection.commit()
        except Exception as e:
            logger.error(f"Not possible to store variable change on database: {e}")

    def store_event_alarm(self, name: str, severity: str, message: str, source_node: str):
        """
        Store a triggered event/alarm on database

        :param name: Name of event triggered
        :param severity: Severity of alarm
        :param message: Message of alarm
        :param source_node: Node id reference of event triggered
        :return: None
        """
        try:
            with sqlite3.connect(self.db_path) as connection:
                cursor = connection.cursor()
                cursor.execute('''
                INSERT INTO events
                (name, severity, message, timestamp, source_node)
                VALUES (?, ?, ?, ?, ?)
                ''', (name, severity, message, datetime.now().isoformat(), source_node))

                connection.commit()
        except Exception as e:
            logger.error(f"Not possible to store event on database: {e}")
