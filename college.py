import logging
import mysql.connector
from fastapi import FastAPI
from dotenv import load_dotenv
import os
# Logging setup
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("tidb-api")

load_dotenv()

# Database config
TIDB_CONFIG = {
    'user': os.environ.get('TIDB_USER'),
    'password': os.environ.get('TIDB_PASSWORD'),
    'host': os.environ.get('TIDB_HOST'),
    'port': int(os.environ.get('TIDB_PORT', 4000)), # Use 4000 as default
    'database': os.environ.get('TIDB_DATABASE')
}

def get_db_connection():
    try:
        conn = mysql.connector.connect(**TIDB_CONFIG)
        return conn
    except mysql.connector.Error as err:
        log.error(f"Error connecting to TiDB: {err}")
        raise

# FastAPI app
app = FastAPI()

@app.get("/get_events_in_duration")
def get_events_in_duration(start_date: str, end_date: str):
    try:
        with get_db_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                query = """
                SELECT * FROM events 
                WHERE event_date BETWEEN %s AND %s 
                ORDER BY event_date, start_time
                """
                cursor.execute(query, (start_date, end_date))
                events = cursor.fetchall()
                return {"events": events}
    except Exception as e:
        return {"error": str(e)}

@app.get("/get_events_by_type")
def get_events_by_type(event_type: str):
    try:
        with get_db_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                query = "SELECT * FROM events WHERE LOWER(event_type) = LOWER(%s)"
                cursor.execute(query, (event_type,))
                events = cursor.fetchall()
                return {"events": events}
    except Exception as e:
        return {"error": str(e)}
