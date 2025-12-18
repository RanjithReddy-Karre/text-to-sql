from config import snowflake_connection
import snowflake.connector

conn = snowflake.connector.connect(
    user=snowflake_connection.user,
    password=snowflake_connection.password,
    account=snowflake_connection.account,          # e.g. xy12345.us-east-1
    warehouse=snowflake_connection.warehouse,
    database=snowflake_connection.database,
    role=snowflake_connection.role
)

cur = conn.cursor()
try:
    cur.execute("select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER limit 100;")
    print(cur.fetchall())
finally:
    cur.close()
    conn.close()

print(snowflake_connection.user)