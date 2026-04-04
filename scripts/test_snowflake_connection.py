from src.snowflake.connector import get_snowflake_connection

conn = get_snowflake_connection()
cur = conn.cursor()

cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA();")

for row in cur.fetchall():
    print(row)

cur.close()
conn.close()

print("✅ connection works")