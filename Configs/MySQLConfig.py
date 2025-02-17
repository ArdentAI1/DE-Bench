import mysql.connector
import os

from dotenv import load_dotenv

load_dotenv()


connection = mysql.connector.connect(
    host=os.getenv("MYSQL_HOST"),
    port=os.getenv("MYSQL_PORT"),
    user=os.getenv("MYSQL_USERNAME"),
    password=os.getenv("MYSQL_PASSWORD"),
    connect_timeout=10,
)

