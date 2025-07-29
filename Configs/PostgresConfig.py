import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# Create a base PostgreSQL connection to system database
connection = psycopg2.connect(
    host=os.getenv("POSTGRES_HOSTNAME"),
    port=os.getenv("POSTGRES_PORT"),
    user=os.getenv("POSTGRES_USERNAME"),
    password=os.getenv("POSTGRES_PASSWORD"),
    database="postgres",  # Default system database
    sslmode="require",
    connect_timeout=10,
)


def confirmPostgresConnection():
    """Test PostgreSQL connection with a simple query."""
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()
        cursor.close()
        # print(f"Successfully connected to PostgreSQL: {version[0]}")
        return True
    except Exception as e:
        print(f"PostgreSQL connection error: {e}")
        return False 