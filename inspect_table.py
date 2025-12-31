import os
from dotenv import load_dotenv
from databricks import sql

load_dotenv()

server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
access_token = os.getenv("DATABRICKS_TOKEN")

try:
    connection = sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token
    )
    with connection.cursor() as cursor:
        print("DESCRIBING index_1:")
        cursor.execute("DESCRIBE index_1")
        columns = cursor.fetchall()
        for col in columns:
            print(col)
            
        print("\nPREVIEW index_1 (5 rows):")
        cursor.execute("SELECT * FROM index_1 LIMIT 5")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
            
    connection.close()
except Exception as e:
    print(f"Error: {e}")
