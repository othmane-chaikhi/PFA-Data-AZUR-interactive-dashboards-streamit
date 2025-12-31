import os
from dotenv import load_dotenv
from databricks import sql

load_dotenv()

server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
access_token = os.getenv("DATABRICKS_TOKEN")

print(f"DEBUG: Hostname={server_hostname}")
print(f"DEBUG: Path={http_path}")
print(f"DEBUG: Token Length={len(access_token) if access_token else 0}")

try:
    print("DEBUG: Connecting...")
    connection = sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token
    )
    print("DEBUG: Connection established.")
    with connection.cursor() as cursor:
        print("DEBUG: Executing SHOW TABLES...")
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print(f"DEBUG: Found {len(tables)} tables.")
        for table in tables:
            print(f"TABLE: {table}")
    connection.close()
except Exception as e:
    print(f"DEBUG: Error: {e}")
