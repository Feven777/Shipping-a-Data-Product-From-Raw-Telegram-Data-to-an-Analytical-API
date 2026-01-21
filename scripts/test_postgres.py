import sys
from pathlib import Path
import socket
print("localhost resolves to:", socket.gethostbyname("localhost"))


# Add project root to sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# Now this import will work
from src.config import Config
import psycopg2

print("Using DSN:", Config.POSTGRES_DSN)

try:
    conn = psycopg2.connect(Config.POSTGRES_DSN)
    cur = conn.cursor()
    cur.execute("SELECT version();")
    print(cur.fetchone())
    cur.close()
    conn.close()
except Exception as e:
    print("Connection failed:", e)
